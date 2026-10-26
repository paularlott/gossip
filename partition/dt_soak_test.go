package partition

// Randomised soak: a workload from several goroutines runs while random faults come and go, then everything is healed and
// the invariants and liveness are checked. Seeded: the seed is printed; PARTITION_SOAK_SEED replays the fault schedule
// (message timing is not reproducible), PARTITION_SOAK_DURATION lengthens the run (default 15s; a nightly or release run
// should use 2-5 minutes, e.g. PARTITION_SOAK_DURATION=3m), PARTITION_SOAK_NODES sets the cluster size (default 3).

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

type dtUndo struct {
	at   time.Time
	what string
	fn   func()
}

func dtEnvInt(t *testing.T, name string, def int64) int64 {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return n
}

func TestDTSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak skipped in -short mode")
	}
	seed := dtEnvInt(t, "PARTITION_SOAK_SEED", time.Now().UnixNano()%1_000_000_007)
	nodes := int(dtEnvInt(t, "PARTITION_SOAK_NODES", 3))
	dur := 15 * time.Second
	if v := os.Getenv("PARTITION_SOAK_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("PARTITION_SOAK_DURATION=%q: %v", v, err)
		}
		dur = d
	}
	t.Logf("soak seed %d (PARTITION_SOAK_SEED=%d), %d nodes, %v", seed, seed, nodes, dur)

	c := dtStartCluster(t, dtOpts{Nodes: nodes, Parts: 6, Seed: seed})
	rng := rand.New(rand.NewSource(seed))
	w := c.startWorkload(dtWLOpts{Workers: 4, Seed: seed})

	var undos []dtUndo
	var history []string
	note := func(format string, args ...any) {
		line := fmt.Sprintf("%6.2fs ", time.Since(c.logs.start).Seconds()) + fmt.Sprintf(format, args...)
		history = append(history, line)
		c.logs.add("SOAK " + line)
	}
	later := func(min, max time.Duration, what string, fn func()) {
		undos = append(undos, dtUndo{at: time.Now().Add(min + time.Duration(rng.Int63n(int64(max-min)))), what: what, fn: fn})
	}
	pickNode := func() int { return rng.Intn(nodes) }
	pickPair := func() (int, int) {
		i := pickNode()
		j := (i + 1 + rng.Intn(nodes-1)) % nodes
		return i, j
	}
	// A disk may be lost only while no other disk is: every node must be up, and a node that lost its disk earlier must
	// have finished resyncing. (With a majority of intact disks the protocol must keep every committed write.)
	lostPending := -1
	canLoseDisk := func(i int) bool {
		if lostPending >= 0 {
			if m, _ := c.node(lostPending); m == nil || !m.Ready() {
				return false
			}
			lostPending = -1
		}
		for j := 0; j < nodes; j++ {
			if j != i && !c.isUp(j) {
				return false
			}
		}
		return true
	}
	restartNode := func(i int, lose bool) {
		if c.isUp(i) {
			return
		}
		if lose && canLoseDisk(i) {
			note("restart %d with its disk lost", i)
			lostPending = i
			c.restart(i, false)
			return
		}
		note("restart %d with its data", i)
		c.restart(i, true)
	}

	deadline := time.Now().Add(dur)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for time.Now().Before(deadline) {
		<-tick.C
		// Undo what is due.
		sort.Slice(undos, func(a, b int) bool { return undos[a].at.Before(undos[b].at) })
		for len(undos) > 0 && !undos[0].at.After(time.Now()) {
			u := undos[0]
			undos = undos[1:]
			note("undo: %s", u.what)
			u.fn()
		}
		if rng.Intn(100) >= 30 {
			continue
		}
		switch r := rng.Intn(100); {
		case r < 12: // cut a node off
			i := pickNode()
			if c.isUp(i) {
				note("cut %d", i)
				c.Cut(i)
				later(300*time.Millisecond, 3*time.Second, fmt.Sprintf("uncut %d", i), func() { c.Uncut(i) })
			}
		case r < 24: // split the cluster
			perm := rng.Perm(nodes)
			k := 1 + rng.Intn(nodes-1)
			a, b := perm[:k], perm[k:]
			note("split %v | %v", a, b)
			c.Split(a, b)
			later(500*time.Millisecond, 3*time.Second, fmt.Sprintf("heal split %v | %v", a, b), func() {
				for _, i := range a {
					for _, j := range b {
						c.Unblock(i, j)
						c.Unblock(j, i)
					}
				}
			})
		case r < 36: // one way block
			i, j := pickPair()
			note("block %d -> %d", i, j)
			c.Block(i, j)
			later(300*time.Millisecond, 3*time.Second, fmt.Sprintf("unblock %d -> %d", i, j), func() { c.Unblock(i, j) })
		case r < 44: // delay with jitter
			i, j := pickPair()
			hi := time.Duration(20+rng.Intn(130)) * time.Millisecond
			note("delay %d -> %d up to %v", i, j, hi)
			c.Delay(i, j, 0, hi)
			later(500*time.Millisecond, 3*time.Second, fmt.Sprintf("undelay %d -> %d", i, j), func() { c.Delay(i, j, 0, 0) })
		case r < 54: // drop requests
			i, j := pickPair()
			p := 0.1 + rng.Float64()*0.5
			note("drop %.2f %d -> %d", p, i, j)
			c.DropRate(i, j, p)
			later(500*time.Millisecond, 3*time.Second, fmt.Sprintf("undrop %d -> %d", i, j), func() { c.DropRate(i, j, 0) })
		case r < 66: // drop replies
			i, j := pickPair()
			p := 0.1 + rng.Float64()*0.9
			note("drop replies %.2f %d <- %d", p, i, j)
			c.DropReply(i, j, p)
			later(500*time.Millisecond, 3*time.Second, fmt.Sprintf("undrop replies %d <- %d", i, j), func() { c.DropReply(i, j, 0) })
		case r < 72: // duplicate packets
			i, j := pickPair()
			note("duplicate %d -> %d", i, j)
			c.Duplicate(i, j, 0.5)
			later(500*time.Millisecond, 3*time.Second, fmt.Sprintf("unduplicate %d -> %d", i, j), func() { c.Duplicate(i, j, 0) })
		case r < 92: // crash a node, with or without its data on the way back
			i := pickNode()
			if c.isUp(i) {
				lose := rng.Intn(3) == 0
				note("crash %d (comes back with its disk lost: %v)", i, lose)
				c.crash(i)
				later(300*time.Millisecond, 4*time.Second, fmt.Sprintf("restart %d", i), func() { restartNode(i, lose) })
			}
		case r < 96: // everything is repaired
			note("heal everything")
			c.Heal()
		default: // everything goes down at once
			if len(c.upNodes()) == nodes {
				note("crash all")
				for i := 0; i < nodes; i++ {
					c.crash(i)
				}
				for i := 0; i < nodes; i++ {
					later(300*time.Millisecond, 2*time.Second, fmt.Sprintf("restart %d", i), func() { restartNode(i, false) })
				}
			}
		}
	}
	note("end of faults")
	w.finish()
	// Settle: heal everything, bring every node back with its data.
	failed := t.Failed()
	defer func() {
		if t.Failed() && !failed {
			t.Logf("soak seed %d; fault history:\n%s", seed, joinLines(history))
		}
	}()
	c.settle()
	c.verify()
}

func joinLines(l []string) string {
	out := ""
	for _, s := range l {
		out += s + "\n"
	}
	return out
}
