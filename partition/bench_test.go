package partition

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip"
	"github.com/paularlott/gossip/internal/faultnet"
	"github.com/paularlott/logger"
)

// The benchmarks run real gossip clusters over the socket transport on localhost (each node's transport wrapped in
// internal/faultnet, which adds network latency when asked) with the in-memory Store of store_test.go. Run them with
//
//	go test -run '^$' -bench 'BenchmarkPartition' -benchtime=1s ./partition/
//
// (the takeover benchmark builds a cluster per iteration: give it -benchtime=3x). Every helper and type of this file is
// prefixed bench, so that it cannot collide with another test file's names.

// benchNode is one node of a benchmark cluster.
type benchNode struct {
	c  *gossip.Cluster
	ft *faultnet.Transport
	id gossip.NodeID
	m  *Manager
}

// benchCluster is n nodes on localhost.
type benchCluster struct {
	b     testing.TB
	nodes []*benchNode
	net   *faultnet.Network
	parts int
	once  sync.Once
}

// stopAll stops every node's Manager and cluster; it is also run when the benchmark ends.
func (bc *benchCluster) stopAll() {
	bc.once.Do(func() {
		var wg sync.WaitGroup
		for _, n := range bc.nodes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n.m.Stop()
				n.c.Stop()
			}()
		}
		wg.Wait()
	})
}

// benchOptions configures benchNewCluster.
type benchOptions struct {
	size       int           // nodes (and ClusterSize)
	parts      int           // partitions
	heartbeat  time.Duration // HeartbeatInterval; the other timings are the defaults derived from it
	syncCost   time.Duration // memStore.syncCost: what SetPromised and SetPending pay (an fsync)
	commitCost time.Duration // memStore.commitCost: what Commit and Import pay (an fsync, if a Store makes them durable)
	workers    int           // gossip.Config.NumIncomingWorkers of every node (0: the library's default)
}

func benchFreeAddr(tb testing.TB) string {
	tb.Helper()
	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		addr := ln.Addr().String()
		udp, err := net.ListenPacket("udp", addr)
		ln.Close()
		if err != nil {
			continue
		}
		udp.Close()
		return addr
	}
	tb.Fatal("no free port")
	return ""
}

// benchNewCluster starts o.size nodes, joins them, and returns once every node is ready and every partition has one
// owner that every node agrees on.
func benchNewCluster(tb testing.TB, o benchOptions) *benchCluster {
	tb.Helper()
	bc := &benchCluster{b: tb, net: faultnet.NewNetwork(), parts: o.parts}
	tb.Cleanup(bc.stopAll)
	var seed string
	for i := range o.size {
		addr := benchFreeAddr(tb)
		cfg := gossip.DefaultConfig()
		cfg.BindAddr = addr
		cfg.AdvertiseAddr = addr
		cfg.Logger = logger.NewNullLogger()
		// Brisk failure detection, as in the integration test.
		cfg.SuspectTimeout = 500 * time.Millisecond
		cfg.DeadNodeTimeout = 2 * time.Second
		cfg.GossipInterval = 250 * time.Millisecond
		if o.workers > 0 {
			cfg.NumIncomingWorkers = o.workers
		}
		ft := faultnet.New(gossip.NewSocketTransport(cfg), int64(i+1))
		cfg.Transport = ft
		c, err := gossip.NewCluster(cfg)
		if err != nil {
			tb.Fatalf("cluster on %s: %v", addr, err)
		}
		c.Start()
		n := &benchNode{c: c, ft: ft, id: c.LocalNode().ID}
		bc.net.Add(n.id, ft)
		bc.net.SetAddress(n.id, addr)
		if i == 0 {
			seed = addr
		} else if err := c.Join([]string{seed}); err != nil {
			tb.Fatalf("node %d join: %v", i, err)
		}
		store := newMemStore()
		store.syncCost, store.commitCost = o.syncCost, o.commitCost
		m, err := New(c, Config{
			Partitions:        o.parts,
			ClusterSize:       o.size,
			HeartbeatInterval: o.heartbeat,
			WriteTimeout:      10 * time.Second,
			Store:             store,
		})
		if err != nil {
			tb.Fatal(err)
		}
		n.m = m
		bc.nodes = append(bc.nodes, n)
	}
	for _, n := range bc.nodes {
		deadline := time.Now().Add(30 * time.Second)
		for n.c.NumAliveNodes() != o.size {
			if time.Now().After(deadline) {
				tb.Fatalf("the cluster never saw %d alive nodes", o.size)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, n := range bc.nodes {
		if err := n.m.Start(); err != nil {
			tb.Fatal(err)
		}
	}
	bc.waitOwners(60*time.Second, bc.nodes...)
	return bc
}

// waitOwners waits until every partition has exactly one owner among live and every live node agrees who it is.
func (bc *benchCluster) waitOwners(limit time.Duration, live ...*benchNode) {
	bc.b.Helper()
	deadline := time.Now().Add(limit)
	for {
		ok := true
	parts:
		for p := 0; p < bc.parts; p++ {
			owners := 0
			var owner gossip.NodeID
			for _, n := range live {
				if n.m.IsOwner(p) {
					owners++
					owner = n.id
				}
			}
			if owners != 1 {
				ok = false
				break
			}
			for _, n := range live {
				if o, known := n.m.Owner(p); !known || o != owner {
					ok = false
					break parts
				}
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			bc.b.Fatalf("partitions never settled on owners")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ownedBy returns the partitions node n owns.
func (bc *benchCluster) ownedBy(n *benchNode) []int {
	var out []int
	for p := 0; p < bc.parts; p++ {
		if n.m.IsOwner(p) {
			out = append(out, p)
		}
	}
	return out
}

// delay makes every request between every pair of nodes take d longer (faultnet delays the request leg of an exchange,
// so a request/response pays d, not 2d). Zero removes it.
func (bc *benchCluster) delay(d time.Duration) {
	for _, a := range bc.nodes {
		for _, b := range bc.nodes {
			if a != b {
				bc.net.Delay(a.id, b.id, d, d)
			}
		}
	}
}

// benchPayload is one change: opaque bytes to the package, 64 of them here.
var benchPayload = make([]byte, 64)

// benchWrite writes through n and fails the benchmark if the write does not commit.
func benchWrite(b *testing.B, n *benchNode, p int) bool {
	if _, err := n.m.Write(context.Background(), p, benchPayload); err != nil {
		b.Errorf("Write(%d): %v", p, err)
		return false
	}
	return true
}

// benchPercentile returns the p-th percentile (0..1) of sorted durations.
func benchPercentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(int(float64(len(sorted))*p), len(sorted)-1)]
}

// benchReport reports the median and 99th percentile latency of the recorded writes.
func benchReport(b *testing.B, lat []time.Duration) {
	slices.Sort(lat)
	b.ReportMetric(float64(benchPercentile(lat, 0.50))/1e3, "p50-µs")
	b.ReportMetric(float64(benchPercentile(lat, 0.99))/1e3, "p99-µs")
}

// benchSequential runs b.N writes one after the other through n to partition p.
func benchSequential(b *testing.B, n *benchNode, p int) {
	for range 20 { // warm up the connections
		benchWrite(b, n, p)
	}
	lat := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for range b.N {
		start := time.Now()
		if !benchWrite(b, n, p) {
			return
		}
		lat = append(lat, time.Since(start))
	}
	b.StopTimer()
	benchReport(b, lat)
}

// benchConcurrent runs b.N writes by `writers` goroutines, spread over the nodes (writer g uses node g mod n) and over
// the partitions at random (or, with onePartition, all to partition 0 through the writers' own nodes). ns/op is the
// wall time per write, so the reported throughput is the aggregate.
func benchConcurrent(b *testing.B, bc *benchCluster, writers int, onePartition bool) {
	for g := range bc.nodes { // warm up the connections between every pair
		for p := 0; p < bc.parts; p++ {
			benchWrite(b, bc.nodes[g], p)
		}
	}
	var next atomic.Int64
	lats := make([][]time.Duration, writers)
	var wg sync.WaitGroup
	b.ResetTimer()
	for g := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := bc.nodes[g%len(bc.nodes)]
			rng := rand.New(rand.NewPCG(uint64(g), 1))
			for next.Add(1) <= int64(b.N) {
				p := 0
				if !onePartition {
					p = rng.IntN(bc.parts)
				}
				start := time.Now()
				if !benchWrite(b, n, p) {
					return
				}
				lats[g] = append(lats[g], time.Since(start))
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	benchReport(b, slices.Concat(lats...))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/s")
}

// BenchmarkPartitionWriteLatency is the latency of one write at a time (3 nodes, no store cost): when the request lands
// on the partition's owner, and when it lands on another node, which has to send it to the owner first. Also the
// throughput of a single partition written sequentially: 1e9 / ns-per-op.
func BenchmarkPartitionWriteLatency(b *testing.B) {
	bc := benchNewCluster(b, benchOptions{size: 3, parts: 16, heartbeat: 100 * time.Millisecond})
	p := bc.ownedBy(bc.nodes[0])[0]
	b.Run("owner", func(b *testing.B) { benchSequential(b, bc.nodes[0], p) })
	b.Run("non-owner", func(b *testing.B) { benchSequential(b, bc.nodes[1], p) })
}

// BenchmarkPartitionWriteConcurrent is the throughput of concurrent writers (3 nodes, no store cost): writers spread
// over the nodes, writing to random partitions out of 16 ("spread"), or all to one partition ("one-partition"), which
// the owner serialises. The heartbeat interval matters here: a change that finds the owner's handlers busy waits a
// quarter of it before it is retried.
func BenchmarkPartitionWriteConcurrent(b *testing.B) {
	for _, hb := range []time.Duration{100 * time.Millisecond, 500 * time.Millisecond} {
		b.Run(fmt.Sprintf("heartbeat=%s", hb), func(b *testing.B) {
			bc := benchNewCluster(b, benchOptions{size: 3, parts: 16, heartbeat: hb})
			for _, writers := range []int{8, 64} {
				b.Run(fmt.Sprintf("spread/writers=%d", writers), func(b *testing.B) { benchConcurrent(b, bc, writers, false) })
			}
			b.Run("one-partition/writers=8", func(b *testing.B) { benchConcurrent(b, bc, 8, true) })
		})
	}
}

// BenchmarkPartitionNetworkDelay adds latency to every request between the nodes (3 nodes, 100ms heartbeat) and
// measures one write at a time on the owner and on another node, and 64 concurrent writers. faultnet delays the request
// leg of an exchange only, so "delay=1ms" means 1ms per request; a write on the owner is one round of requests to the
// followers, a write on another node is that plus the request to the owner.
func BenchmarkPartitionNetworkDelay(b *testing.B) {
	bc := benchNewCluster(b, benchOptions{size: 3, parts: 16, heartbeat: 100 * time.Millisecond})
	p := bc.ownedBy(bc.nodes[0])[0]
	for _, d := range []time.Duration{0, time.Millisecond, 5 * time.Millisecond} {
		b.Run(fmt.Sprintf("delay=%s", d), func(b *testing.B) {
			bc.delay(d)
			b.Cleanup(func() { bc.delay(0) })
			b.Run("owner", func(b *testing.B) { benchSequential(b, bc.nodes[0], p) })
			b.Run("non-owner", func(b *testing.B) { benchSequential(b, bc.nodes[1], p) })
			b.Run("spread/writers=64", func(b *testing.B) { benchConcurrent(b, bc, 64, false) })
		})
	}
}

// BenchmarkPartitionClusterSize compares 3 and 5 nodes (100ms heartbeat, no store cost): one write at a time on the
// owner and on another node, and 64 concurrent writers. A write needs a majority: 2 of 3, 3 of 5.
func BenchmarkPartitionClusterSize(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			bc := benchNewCluster(b, benchOptions{size: size, parts: 16, heartbeat: 100 * time.Millisecond})
			p := bc.ownedBy(bc.nodes[0])[0]
			b.Run("owner", func(b *testing.B) { benchSequential(b, bc.nodes[0], p) })
			b.Run("non-owner", func(b *testing.B) { benchSequential(b, bc.nodes[1], p) })
			b.Run("spread/writers=64", func(b *testing.B) { benchConcurrent(b, bc, 64, false) })
		})
	}
}

// BenchmarkPartitionStoreCost adds a cost to the Store's writes, the part a real fsync plays: 0, 1ms (a fast disk) and
// 4ms (a full fsync on a laptop SSD). SetPromised and SetPending always pay it (they must be durable); "commit=durable" has
// Commit and Import pay it too (a Store that fsyncs them), "commit=lazy" does not (the contract: Commit need not be
// durable). 3 nodes; one write at a time on the owner, and 64 concurrent writers.
func BenchmarkPartitionStoreCost(b *testing.B) {
	type variant struct {
		cost    time.Duration
		durable bool
	}
	var variants []variant
	for _, cost := range []time.Duration{0, time.Millisecond, 4 * time.Millisecond} {
		variants = append(variants, variant{cost, false})
		if cost > 0 {
			variants = append(variants, variant{cost, true})
		}
	}
	for _, v := range variants {
		name := fmt.Sprintf("cost=%s/commit=lazy", v.cost)
		o := benchOptions{size: 3, parts: 16, heartbeat: 100 * time.Millisecond, syncCost: v.cost}
		if v.durable {
			name = fmt.Sprintf("cost=%s/commit=durable", v.cost)
			o.commitCost = v.cost
		}
		b.Run(name, func(b *testing.B) {
			bc := benchNewCluster(b, o)
			p := bc.ownedBy(bc.nodes[0])[0]
			b.Run("owner", func(b *testing.B) { benchSequential(b, bc.nodes[0], p) })
			b.Run("spread/writers=64", func(b *testing.B) { benchConcurrent(b, bc, 64, false) })
		})
	}
}

// benchTimeToWritable probes writes to every partition in lost through n, one probe per partition every 25ms (each with
// its own timeout, so that a probe stuck on the lost owner does not hold the next one back), and returns how long after
// start the first and the last of the partitions accepted a write.
func benchTimeToWritable(b *testing.B, n *benchNode, lost []int, start time.Time) (first, last time.Duration) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, p := range lost {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			done := make(chan struct{})
			var once sync.Once
			t := time.NewTicker(25 * time.Millisecond)
			defer t.Stop()
			for {
				go func() {
					actx, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()
					if _, err := n.m.Write(actx, p, benchPayload); err == nil {
						once.Do(func() {
							d := time.Since(start)
							mu.Lock()
							if first == 0 || d < first {
								first = d
							}
							last = max(last, d)
							mu.Unlock()
							close(done)
						})
					}
				}()
				select {
				case <-done:
					return
				case <-ctx.Done():
					b.Errorf("partition %d never became writable again", p)
					return
				case <-t.C:
				}
			}
		}()
	}
	wg.Wait()
	return first, last
}

// BenchmarkPartitionTakeover measures how long partitions are without a writable owner. The owner of some partitions
// of a 3 node cluster is lost, either abruptly ("crash": its transport is cut and nothing is announced) or by stopping
// its Manager ("stop": it hands its partitions back), and a surviving node probes writes to each of them. ns/op is the
// time from the loss until the last of the lost partitions accepted a write; "first-ms" is the time until the first
// did, and, for a crash, "node-dead-ms" the time until the survivor's cluster stopped counting the lost node as alive
// (the library's failure detector, which the takeover has to wait for). Every iteration builds a new cluster (outside the timing): run it with -benchtime=3x.
func BenchmarkPartitionTakeover(b *testing.B) {
	type variant struct {
		name  string
		hb    time.Duration
		crash bool // else the Manager is stopped
	}
	for _, v := range []variant{
		{"heartbeat=100ms/crash", 100 * time.Millisecond, true},
		{"heartbeat=100ms/stop", 100 * time.Millisecond, false},
		{"heartbeat=500ms/crash", 500 * time.Millisecond, true},
		{"heartbeat=500ms/stop", 500 * time.Millisecond, false},
	} {
		b.Run(v.name, func(b *testing.B) {
			var firstSum, detectSum time.Duration
			for range b.N {
				b.StopTimer()
				bc := benchNewCluster(b, benchOptions{size: 3, parts: 16, heartbeat: v.hb})
				victim, survivor := bc.nodes[0], bc.nodes[1]
				lost := bc.ownedBy(victim)
				if len(lost) == 0 {
					b.Fatal("the victim owns nothing")
				}
				// detected: when the survivor's cluster stops counting the victim as alive, which takes the candidates'
				// view (and so the rendezvous owner) off the lost node.
				detected := make(chan time.Duration, 1)
				stopWatch := make(chan struct{})
				b.StartTimer()
				start := time.Now()
				go func() {
					for {
						select {
						case <-stopWatch:
							return
						case <-time.After(2 * time.Millisecond):
						}
						if nd := survivor.c.GetNode(victim.id); nd == nil || nd.GetObservedState() != gossip.NodeAlive {
							detected <- time.Since(start)
							return
						}
					}
				}()
				if v.crash {
					victim.ft.Cut()
				} else {
					victim.m.Stop()
				}
				first, _ := benchTimeToWritable(b, survivor, lost, start)
				b.StopTimer() // the timed region ran until the last lost partition accepted a write
				close(stopWatch)
				firstSum += first
				select {
				case d := <-detected:
					detectSum += d
				default:
				}
				bc.stopAll()
			}
			b.ReportMetric(float64(firstSum.Milliseconds())/float64(b.N), "first-ms")
			if v.crash {
				b.ReportMetric(float64(detectSum.Milliseconds())/float64(b.N), "node-dead-ms")
			}
		})
	}
}

// benchReadReq is the read of the benchmarks: the Store's committed sequence number (cheap, so the figures are the cost of
// the protocol: the confirmation round and the hop to the owner).
var benchReadReq = []byte("seq")

// benchRead reads through n and fails the benchmark if the read is not answered.
func benchRead(b *testing.B, n *benchNode, p int) bool {
	if _, err := n.m.Read(context.Background(), p, benchReadReq); err != nil {
		b.Errorf("Read(%d): %v", p, err)
		return false
	}
	return true
}

// benchReadSequential runs b.N reads one after the other through n of partition p.
func benchReadSequential(b *testing.B, n *benchNode, p int) {
	for range 20 { // warm up the connections
		benchRead(b, n, p)
	}
	lat := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for range b.N {
		start := time.Now()
		if !benchRead(b, n, p) {
			return
		}
		lat = append(lat, time.Since(start))
	}
	b.StopTimer()
	benchReport(b, lat)
}

// benchReadConcurrent runs b.N reads by `readers` goroutines, spread over the nodes (reader g uses node g mod n) and over
// the partitions at random. ns/op is the wall time per read, so the reported throughput is the aggregate.
func benchReadConcurrent(b *testing.B, bc *benchCluster, readers int) {
	for g := range bc.nodes { // warm up the connections between every pair
		for p := 0; p < bc.parts; p++ {
			benchRead(b, bc.nodes[g], p)
		}
	}
	rounds0 := make([]int64, len(bc.nodes))
	for i, n := range bc.nodes {
		rounds0[i] = n.m.readRounds.Load()
	}
	var next atomic.Int64
	lats := make([][]time.Duration, readers)
	var wg sync.WaitGroup
	b.ResetTimer()
	for g := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := bc.nodes[g%len(bc.nodes)]
			rng := rand.New(rand.NewPCG(uint64(g), 1))
			for next.Add(1) <= int64(b.N) {
				p := rng.IntN(bc.parts)
				start := time.Now()
				if !benchRead(b, n, p) {
					return
				}
				lats[g] = append(lats[g], time.Since(start))
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	benchReport(b, slices.Concat(lats...))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/s")
	var rounds int64
	for i, n := range bc.nodes {
		rounds += n.m.readRounds.Load() - rounds0[i]
	}
	b.ReportMetric(float64(rounds)/float64(b.N), "rounds/read")
}

// BenchmarkPartitionRead is the cost of a linearizable read (3 nodes, 100ms heartbeat, no store cost): one read at a time
// when it lands on the partition's owner and when it lands on another node (which sends it to the owner first), the same
// with 1ms added to every request, and 64 concurrent readers spread over the nodes and the 16 partitions, whose reads
// share confirmation rounds ("rounds/read" is how many rounds the owners ran per read).
func BenchmarkPartitionRead(b *testing.B) {
	bc := benchNewCluster(b, benchOptions{size: 3, parts: 16, heartbeat: 100 * time.Millisecond})
	p := bc.ownedBy(bc.nodes[0])[0]
	b.Run("owner", func(b *testing.B) { benchReadSequential(b, bc.nodes[0], p) })
	b.Run("non-owner", func(b *testing.B) { benchReadSequential(b, bc.nodes[1], p) })
	b.Run("delay=1ms/owner", func(b *testing.B) {
		bc.delay(time.Millisecond)
		defer bc.delay(0)
		benchReadSequential(b, bc.nodes[0], p)
	})
	b.Run("delay=1ms/non-owner", func(b *testing.B) {
		bc.delay(time.Millisecond)
		defer bc.delay(0)
		benchReadSequential(b, bc.nodes[1], p)
	})
	b.Run("concurrent/readers=64", func(b *testing.B) { benchReadConcurrent(b, bc, 64) })
}
