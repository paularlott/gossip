package partition

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/paularlott/gossip"
)

// peerInfo is what the Manager knows about another alive node from its metadata.
type peerInfo struct {
	ID      gossip.NodeID
	Size    int
	Cluster string
	Ready   bool
}

// errNotSent marks a transport error where the request certainly never reached the peer, so an operation may be retried
// without any risk of it having happened: the transport's error wraps a *net.OpError whose Op is "dial" (the connection
// could not be made; a transport wrapper that refuses to send reports it the same way). Any other error may have come
// after the request left, and counts as possibly delivered.
var errNotSent = errors.New("partition: request not sent")

// network is everything the Manager needs from the cluster. clusterNet implements it over a *gossip.Cluster; tests
// substitute an in-process network.
type network interface {
	// self returns the local node's ID.
	self() gossip.NodeID
	// peers returns the alive nodes other than this one.
	peers() []peerInfo
	// publish sets the local node's metadata.
	publish(size int, cluster string, ready bool)
	// register installs the protocol's handlers, routing to m; unregister removes them.
	register(m *Manager) error
	unregister()

	// call sends req to a node as a request of type mt and decodes the reply into resp. An error that happened before
	// the request could have been delivered wraps errNotSent.
	call(ctx context.Context, to gossip.NodeID, mt gossip.MessageType, req, resp interface{}) error
	// send is the one-way, best-effort send of relinquish and commit notices.
	send(to gossip.NodeID, mt gossip.MessageType, msg interface{}) error
	// catchUp opens the catch-up stream; the caller reads the header frame and then the body, and closes it.
	catchUp(ctx context.Context, to gossip.NodeID, req *catchUpReq) (io.ReadCloser, error)
}

// reply is one node's answer to a request; resp is nil if the node did not answer.
type reply[T any] struct {
	from gossip.NodeID
	resp *T
}

// ask sends req to a node and returns its reply, decoded as a T.
func ask[T any](ctx context.Context, nw network, to gossip.NodeID, mt gossip.MessageType, req interface{}) (*T, error) {
	resp := new(T)
	if err := nw.call(ctx, to, mt, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// askAll sends req to every other alive node in parallel. It returns how many nodes it asked and the channel their
// replies arrive on, exactly one per node and in the order they come in (a nil resp for a node that did not answer).
// The channel is buffered, so a caller that stops reading early leaks nothing; the requests end with ctx.
func askAll[T any](ctx context.Context, nw network, mt gossip.MessageType, req interface{}) (int, <-chan reply[T]) {
	peers := nw.peers()
	out := make(chan reply[T], len(peers))
	for _, pi := range peers {
		go func() {
			resp, _ := ask[T](ctx, nw, pi.ID, mt, req)
			out <- reply[T]{pi.ID, resp}
		}()
	}
	return len(peers), out
}

// clusterNet is the network over a real cluster.
type clusterNet struct {
	c *gossip.Cluster
}

func (n *clusterNet) self() gossip.NodeID { return n.c.LocalNode().ID }

func (n *clusterNet) peers() []peerInfo {
	self := n.self()
	var out []peerInfo
	for _, nd := range n.c.AliveNodes() {
		if nd.ID == self {
			continue
		}
		out = append(out, peerInfo{ID: nd.ID, Size: nd.Metadata.GetInt(metaSize), Cluster: nd.Metadata.GetString(metaCluster),
			Ready: nd.Metadata.GetBool(metaReady)})
	}
	return out
}

func (n *clusterNet) publish(size int, cluster string, ready bool) {
	md := n.c.LocalMetadata()
	md.SetInt(metaSize, size)
	md.SetString(metaCluster, cluster)
	md.SetBool(metaReady, ready)
}

// node resolves an ID to a node that is not known to be down.
func (n *clusterNet) node(id gossip.NodeID) (*gossip.Node, error) {
	nd := n.c.GetNode(id)
	if nd == nil {
		return nil, fmt.Errorf("%w: unknown node %s", errNotSent, id)
	}
	return nd, nil
}

func (n *clusterNet) call(ctx context.Context, to gossip.NodeID, mt gossip.MessageType, req, resp interface{}) error {
	nd, err := n.node(to)
	if err != nil {
		return err
	}
	if err := n.c.SendToWithResponseContext(ctx, nd, mt, req, resp); err != nil {
		var op *net.OpError
		if errors.As(err, &op) && op.Op == "dial" {
			return fmt.Errorf("%w: %w", errNotSent, err)
		}
		return err
	}
	return nil
}

func (n *clusterNet) send(to gossip.NodeID, mt gossip.MessageType, msg interface{}) error {
	nd, err := n.node(to)
	if err != nil {
		return err
	}
	return n.c.SendTo(nd, mt, msg)
}

func (n *clusterNet) catchUp(ctx context.Context, to gossip.NodeID, req *catchUpReq) (io.ReadCloser, error) {
	nd, err := n.node(to)
	if err != nil {
		return nil, err
	}
	return n.c.OpenStream(ctx, nd, partCatchUpMsg, req)
}

func (n *clusterNet) unregister() {
	for _, mt := range []gossip.MessageType{partHeartbeatMsg, partPromiseMsg, partWriteMsg, partChangeMsg,
		partStateMsg, partCatchUpMsg, partRelinquishMsg, partCommitMsg, partClaimMsg, partReadMsg} {
		n.c.UnregisterMessageType(mt)
	}
}

// registrar installs the protocol's handlers one message type at a time and remembers which, so that a failed Start
// removes only what it added: the handlers of another Manager on the cluster must survive.
type registrar struct {
	c    *gossip.Cluster
	m    *Manager
	done []gossip.MessageType
}

// add records mt once its handler is registered. A failure to register panics, which register turns into its error;
// the cluster itself panics when mt already has a handler.
func (r *registrar) add(mt gossip.MessageType, err error) {
	if err != nil {
		panic(err)
	}
	r.done = append(r.done, mt)
}

func (n *clusterNet) register(m *Manager) (err error) {
	r := &registrar{c: n.c, m: m}
	defer func() {
		if p := recover(); p != nil {
			for _, mt := range r.done {
				n.c.UnregisterMessageType(mt)
			}
			err = fmt.Errorf("partition: cannot register handlers (a Manager is already running on this cluster?): %v", p)
		}
	}()

	// The second argument is the limit on the requests of the kind and the third what a request over it is answered
	// with: never an acknowledgement.
	request(r, partHeartbeatMsg, m.reqSem, &heartbeatResp{}, m.handleHeartbeat)
	request(r, partPromiseMsg, m.reqSem, &promiseResp{}, m.handlePromise)
	request(r, partWriteMsg, m.reqSem, &writeResp{}, m.handleWrite)
	request(r, partStateMsg, m.reqSem, &stateResp{}, func(_ gossip.NodeID, req *stateReq) *stateResp { return m.handleState(req) })
	request(r, partClaimMsg, m.reqSem, &claimResp{}, m.handleClaim)
	// Changes and reads have limits of their own, which their handlers apply.
	request(r, partChangeMsg, nil, &changeResp{Status: statusUnavailable},
		func(_ gossip.NodeID, req *changeReq) *changeResp { return m.handleChange(m.ctx, req) })
	request(r, partReadMsg, nil, &changeResp{Status: statusUnavailable},
		func(_ gossip.NodeID, req *readReq) *changeResp { return m.handleRead(m.ctx, req) })
	oneWay(r, partRelinquishMsg, m.handleRelinquish)
	oneWay(r, partCommitMsg, m.handleCommit)
	r.add(partCatchUpMsg, n.c.HandleStreamFunc(partCatchUpMsg, func(_ *gossip.Node, pkt *gossip.Packet, w io.Writer) error {
		var req catchUpReq
		if err := pkt.Unmarshal(&req); err != nil {
			return err
		}
		return m.serveCatchUp(&req, w)
	}))
	return nil
}

// request registers the handler of a request that is answered later (gossip.Cluster.HandleFuncWithDeferredReply): it
// decodes the request and hands the work to a goroutine of the Manager, so that no handler holds one of the cluster's
// incoming workers while it waits for the Store, a partition's mutex or other nodes. sem bounds the requests of its
// kind (nil: handle bounds itself) and refuse is the reply of a request that cannot be served.
func request[Req, Resp any](r *registrar, mt gossip.MessageType, sem chan struct{}, refuse *Resp,
	handle func(from gossip.NodeID, req *Req) *Resp) {
	r.add(mt, r.c.HandleFuncWithDeferredReply(mt, func(s *gossip.Node, pkt *gossip.Packet, rep gossip.Replier) {
		var req Req
		if err := pkt.Unmarshal(&req); err != nil {
			rep.Reply(nil, err)
			return
		}
		from := s.ID
		r.m.serve(rep, sem, refuse, func() interface{} { return handle(from, &req) })
	}))
}

// oneWay registers the handler of a message that has no reply.
func oneWay[Msg any](r *registrar, mt gossip.MessageType, handle func(from gossip.NodeID, msg *Msg)) {
	r.add(mt, r.c.HandleFunc(mt, func(s *gossip.Node, pkt *gossip.Packet) error {
		var msg Msg
		if err := pkt.Unmarshal(&msg); err != nil {
			return err
		}
		handle(s.ID, &msg)
		return nil
	}))
}

// serve runs work in a goroutine of the Manager and answers through r with what it returns, so that the cluster's
// incoming worker that delivered the request is free again at once. A request that cannot be served is answered at
// once with refuse, the handler's "not OK" reply: when sem is not nil and its limit is reached, or when the Manager is
// stopping. A refusal is never an acknowledgement.
func (m *Manager) serve(r gossip.Replier, sem chan struct{}, refuse interface{}, work func() interface{}) {
	release := func() {}
	if sem != nil {
		select {
		case sem <- struct{}{}:
			release = func() { <-sem }
		default:
			r.Reply(refuse, nil)
			return
		}
	}
	if !m.spawn(func() {
		defer release()
		r.Reply(work(), nil)
	}) {
		release()
		r.Reply(refuse, nil)
	}
}
