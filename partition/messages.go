package partition

import "github.com/paularlott/gossip"

// Every message carries Size, the sender's ClusterSize; a receiver configured differently answers with
// SizeMismatch and otherwise ignores the message; likewise for one with a different ClusterID (Cluster).

// heartbeatEntry is one partition an owner announces: the epoch it leads at and its committed position.
type heartbeatEntry struct {
	P         int   `msgpack:"p" json:"p"`
	Epoch     Epoch `msgpack:"e" json:"e"`
	Committed Pos   `msgpack:"c" json:"c"`
}

// heartbeatMsg (PartHeartbeatMsg) announces the partitions its sender leads. It is a request only so that the sender
// learns that it arrived, which is what its majority-contact check counts: the library's one-way sends are queued and
// never report a failed delivery.
type heartbeatMsg struct {
	Size    int    `msgpack:"z" json:"z"`
	Cluster string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	// From is carried for compatibility: receivers use the packet's sender.
	From    gossip.NodeID    `msgpack:"f" json:"f"`
	Entries []heartbeatEntry `msgpack:"e" json:"e"`
}

// heartbeatResp acknowledges a heartbeat. Fenced lists the entries the receiver ignored because it has promised a
// higher epoch than the owner's (a deposed owner, or a proposal of the receiver's own that failed): the owner steps
// down on hearing it, as it does when a write's reply shows a higher promise, and takes over again at a higher epoch,
// instead of finding out on its next write.
//
// Accepted lists the partitions whose entry the receiver took as the current owner's: it was not in resync mode, held no
// promise above the entry's epoch when it looked, knew of no owner at a higher epoch and did not lead the partition
// itself at one. It is what a read's leadership confirmation counts (see Manager.confirmLeadership): OK alone is not
// enough, because a receiver answers OK to an entry it ignored for other reasons (it could not get the partition's mutex,
// or is resyncing and has forgotten its promises).
type heartbeatResp struct {
	OK           bool             `msgpack:"ok" json:"ok"`
	SizeMismatch bool             `msgpack:"sm,omitempty" json:"sm,omitempty"`
	Fenced       []heartbeatFence `msgpack:"fe,omitempty" json:"fe,omitempty"`
	Accepted     []int            `msgpack:"ac,omitempty" json:"ac,omitempty"`
}

// heartbeatFence is one partition whose heartbeat entry was ignored, and the promise that outranks it.
type heartbeatFence struct {
	P        int   `msgpack:"p" json:"p"`
	Promised Epoch `msgpack:"pr" json:"pr"`
}

// promiseReq (PartPromiseMsg) asks a node to promise not to accept writes of lower epochs than Epoch.
type promiseReq struct {
	Size    int    `msgpack:"z" json:"z"`
	Cluster string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	P       int    `msgpack:"p" json:"p"`
	Epoch   Epoch  `msgpack:"e" json:"e"`
}

type promiseResp struct {
	OK           bool   `msgpack:"ok" json:"ok"`
	SizeMismatch bool   `msgpack:"sm,omitempty" json:"sm,omitempty"`
	Promised     Epoch  `msgpack:"pr" json:"pr"`
	Committed    Pos    `msgpack:"c" json:"c"`
	Pending      *Write `msgpack:"pe,omitempty" json:"pe,omitempty"`
}

// writeReq (PartWriteMsg) replicates a write; OwnerCommitted lets the receiver settle its own pending write.
type writeReq struct {
	Size           int    `msgpack:"z" json:"z"`
	Cluster        string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	Write          Write  `msgpack:"w" json:"w"`
	OwnerCommitted Pos    `msgpack:"oc" json:"oc"`
}

type writeResp struct {
	OK           bool  `msgpack:"ok" json:"ok"`
	SizeMismatch bool  `msgpack:"sm,omitempty" json:"sm,omitempty"`
	Promised     Epoch `msgpack:"pr" json:"pr"`
	Committed    Pos   `msgpack:"c" json:"c"`
	NeedCatchUp  bool  `msgpack:"nc,omitempty" json:"nc,omitempty"`
}

// changeStatus is the outcome of a PartChangeMsg.
type changeStatus uint8

const (
	statusDone        changeStatus = iota + 1 // committed
	statusRefused                             // the Store refused it; nothing written
	statusUnknown                             // replication began and the outcome is not known
	statusNotOwner                            // the node does not hold the partition; nothing written
	statusUnavailable                         // no majority or a failure before replication; nothing written
	// statusExhausted: a counter would have reached MaxUint64 (ErrExhausted); nothing written. It is a status of its
	// own so that the landing node fails at once instead of retrying until its timeout.
	statusExhausted
	// statusBusy: the owner is running as many forwarded changes as Config.Tuning.MaxConcurrentChanges allows; nothing was
	// run, and the landing node retries after a short pause.
	statusBusy
)

// changeReq (PartChangeMsg) asks the owner to run a change. Deadline is the requester's deadline (Unix nanoseconds; 0:
// none), after which it no longer waits for the answer: the owner refuses, with nothing done, a change it cannot start
// well before it (see Manager.startable), so that it never begins a write whose requester has given up.
type changeReq struct {
	Size     int    `msgpack:"z" json:"z"`
	Cluster  string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	P        int    `msgpack:"p" json:"p"`
	Change   []byte `msgpack:"ch" json:"ch"`
	Deadline int64  `msgpack:"dl,omitempty" json:"dl,omitempty"`
}

// changeResp answers a changeReq. On statusDone, Pos and Reply are the committed write's position and what Prepare
// returned for the caller; on statusRefused, RefusedCode and RefusedMsg are the Store's refusal and Pos the owner's
// committed position when it refused (RefusedError.Pos).
type changeResp struct {
	Status       changeStatus  `msgpack:"st" json:"st"`
	RefusedCode  string        `msgpack:"rc,omitempty" json:"rc,omitempty"`
	RefusedMsg   string        `msgpack:"rm,omitempty" json:"rm,omitempty"`
	Pos          Pos           `msgpack:"pos" json:"pos"`
	Reply        []byte        `msgpack:"rp,omitempty" json:"rp,omitempty"`
	Owner        gossip.NodeID `msgpack:"o" json:"o"`
	Epoch        Epoch         `msgpack:"e" json:"e"`
	SizeMismatch bool          `msgpack:"sm,omitempty" json:"sm,omitempty"`
}

// readReq (PartReadMsg) asks the owner of P to answer Req from P's committed data (Store.Read) once it has confirmed that
// it still owns P. The reply is a changeResp: on statusDone, Reply is what Store.Read returned; on statusRefused,
// RefusedCode and RefusedMsg are the Store's refusal; statusNotOwner, statusBusy and statusUnavailable are as for a change.
// Deadline is the requester's, as in a changeReq.
type readReq struct {
	Size     int    `msgpack:"z" json:"z"`
	Cluster  string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	P        int    `msgpack:"p" json:"p"`
	Req      []byte `msgpack:"rq" json:"rq"`
	Deadline int64  `msgpack:"dl,omitempty" json:"dl,omitempty"`
}

// stateReq (PartStateMsg) asks for every partition's committed position and promise.
type stateReq struct {
	Size    int    `msgpack:"z" json:"z"`
	Cluster string `msgpack:"cl,omitempty" json:"cl,omitempty"`
}

type stateResp struct {
	SizeMismatch bool `msgpack:"sm,omitempty" json:"sm,omitempty"`
	// Intact is false while the replier is in resync mode (its data lost or restored and not yet caught up): its state
	// may lack promises it made and writes it acknowledged, so a node synchronising at startup counts only intact
	// answers toward its quorum and takes nothing from the others.
	Intact bool `msgpack:"in,omitempty" json:"in,omitempty"`
	// Unformed is true while the replier has not formed (Config.Formed). Absence means formed, the safe default for a
	// peer that does not say.
	Unformed  bool    `msgpack:"uf,omitempty" json:"uf,omitempty"`
	Committed []Pos   `msgpack:"c" json:"c"`
	Promised  []Epoch `msgpack:"pr" json:"pr"`
	// Pending is the replier's pending write per partition (nil entries: none); omitted when it has none at all. A node
	// that is resyncing after losing its data adopts the highest-epoch pending write at the position after the
	// committed one, because it may be the only surviving copy of a write that was committed on the lost disk.
	Pending []*Write `msgpack:"pe,omitempty" json:"pe,omitempty"`
}

// catchUpReq (PartCatchUpMsg) opens a stream: a header frame (catchUpHeader) then the Store.Export body.
type catchUpReq struct {
	Size    int    `msgpack:"z" json:"z"`
	Cluster string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	P       int    `msgpack:"p" json:"p"`
	After   uint64 `msgpack:"a" json:"a"`
	Full    bool   `msgpack:"f,omitempty" json:"f,omitempty"`
}

// catchUpHeader is the first frame of a catch-up stream.
type catchUpHeader struct {
	UpTo Pos    `json:"upto"`
	Err  string `json:"err,omitempty"`
}

// relinquishMsg (PartRelinquishMsg) tells the nodes that its sender stepped down voluntarily from P at Epoch.
type relinquishMsg struct {
	Size    int    `msgpack:"z" json:"z"`
	Cluster string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	P       int    `msgpack:"p" json:"p"`
	Epoch   Epoch  `msgpack:"e" json:"e"`
}

// commitMsg (PartCommitMsg) is the owner's notice that it committed a write: one-way, and treated by a receiver exactly
// like one heartbeat entry (settle, fence, catch-up trigger), so that followers apply the write within one message delay
// instead of waiting for the next write or heartbeat.
type commitMsg struct {
	Size      int    `msgpack:"z" json:"z"`
	Cluster   string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	P         int    `msgpack:"p" json:"p"`
	Epoch     Epoch  `msgpack:"e" json:"e"`
	Committed Pos    `msgpack:"c" json:"c"`
}

// claimReq (PartClaimMsg) is a rank 0 node asking the owner of P to hand it over.
type claimReq struct {
	Size    int    `msgpack:"z" json:"z"`
	Cluster string `msgpack:"cl,omitempty" json:"cl,omitempty"`
	P       int    `msgpack:"p" json:"p"`
}

// claimResp answers a claimReq: OK if the owner stepped down.
type claimResp struct {
	OK           bool  `msgpack:"ok" json:"ok"`
	SizeMismatch bool  `msgpack:"sm,omitempty" json:"sm,omitempty"`
	Epoch        Epoch `msgpack:"e,omitempty" json:"e,omitempty"` // the tenure the owner ended (OK only)
}
