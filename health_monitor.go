package gossip

import (
	"context"
	"sync"
	"time"

	"github.com/paularlott/gossip/hlc"
)

type HealthCheckType int

const (
	DirectPing HealthCheckType = iota
	SuspectRetry
	DeadNodeRetry
)

type HealthCheckTask struct {
	NodeID    NodeID
	TaskType  HealthCheckType
	Timestamp hlc.Timestamp
}

type HealthMonitor struct {
	cluster   *Cluster
	taskQueue chan HealthCheckTask
	ctx       context.Context
	wg        *sync.WaitGroup
}

func newHealthMonitor(cluster *Cluster) *HealthMonitor {
	return &HealthMonitor{
		cluster:   cluster,
		taskQueue: make(chan HealthCheckTask, cluster.config.HealthCheckQueueDepth),
		ctx:       cluster.shutdownContext,
		wg:        &cluster.shutdownWg,
	}
}

func (hm *HealthMonitor) start() {
	// Start worker pool
	for i := 0; i < hm.cluster.config.HealthWorkerPoolSize; i++ {
		hm.wg.Add(1)
		go hm.worker()
	}

	// Start health scanner
	hm.wg.Add(1)
	go hm.scanner()

	// Start suspect retry scheduler
	hm.wg.Add(1)
	go hm.suspectRetryScheduler()

	// Start dead node retry scheduler
	hm.wg.Add(1)
	go hm.deadNodeRetryScheduler()
}

func (hm *HealthMonitor) scanner() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.cluster.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.scanNodes()
		case <-hm.ctx.Done():
			return
		}
	}
}

func (hm *HealthMonitor) scanNodes() {
	now := hlc.Now().Time()
	suspectThreshold := time.Duration(hm.cluster.config.SuspectTimeout)

	aliveNodes := hm.cluster.nodes.getAllInStates([]NodeState{NodeAlive})
	for _, node := range aliveNodes {
		if node.ID == hm.cluster.localNode.ID {
			continue
		}

		timeSinceLastMessage := now.Sub(node.getLastActivity().Time())
		if timeSinceLastMessage > suspectThreshold {
			hm.enqueueHealthCheck(node.ID, DirectPing)
		}
	}
}

func (hm *HealthMonitor) suspectRetryScheduler() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.cluster.config.SuspectRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.retrySuspectNodes()
		case <-hm.ctx.Done():
			return
		}
	}
}

func (hm *HealthMonitor) retrySuspectNodes() {
	suspectNodes := hm.cluster.nodes.getAllInStates([]NodeState{NodeSuspect})
	for _, node := range suspectNodes {
		hm.enqueueHealthCheck(node.ID, SuspectRetry)
	}
}

func (hm *HealthMonitor) deadNodeRetryScheduler() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.cluster.config.DeadNodeRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.retryDeadNodes()
		case <-hm.ctx.Done():
			return
		}
	}
}

func (hm *HealthMonitor) retryDeadNodes() {
	deadNodes := hm.cluster.nodes.getAllInStates([]NodeState{NodeDead})
	for _, node := range deadNodes {
		// Only retry dead nodes that haven't been dead too long
		timeSinceDead := hlc.Now().Time().Sub(node.localStateTimestamp.Time())
		if timeSinceDead < hm.cluster.config.MaxDeadNodeRetryTime {
			hm.enqueueHealthCheck(node.ID, DeadNodeRetry)
		}
	}
}

func (hm *HealthMonitor) enqueueHealthCheck(nodeID NodeID, taskType HealthCheckType) {
	task := HealthCheckTask{
		NodeID:    nodeID,
		TaskType:  taskType,
		Timestamp: hlc.Now(),
	}

	select {
	case hm.taskQueue <- task:
	default:
		hm.cluster.config.Logger.Warnf("gossip: Health check queue full, dropping task for node %s", nodeID.String())
	}
}

func (hm *HealthMonitor) worker() {
	defer hm.wg.Done()

	for {
		select {
		case task := <-hm.taskQueue:
			hm.processHealthCheck(task)
		case <-hm.ctx.Done():
			return
		}
	}
}

func (hm *HealthMonitor) processHealthCheck(task HealthCheckTask) {
	node := hm.cluster.nodes.get(task.NodeID)
	if node == nil {
		return
	}

	success := hm.pingNode(node)

	switch task.TaskType {
	case DirectPing:
		if success {
			// Node responded, update last activity
			node.updateLastActivity()
		} else {
			// No response, mark as suspect
			hm.cluster.nodes.updateState(node.ID, NodeSuspect, nil)
			hm.cluster.config.Logger.Debugf("gossip: Marked node as suspect: %s", node.ID.String())
		}

	case SuspectRetry:
		if success {
			// Node recovered, mark as alive
			hm.cluster.nodes.updateState(node.ID, NodeAlive, nil)
			node.updateLastActivity()
			hm.cluster.config.Logger.Debugf("gossip: Node recovered from suspect: %s", node.ID.String())
		} else {
			// Still no response, check if should mark as dead
			timeSinceSuspect := hlc.Now().Time().Sub(node.localStateTimestamp.Time())
			if timeSinceSuspect > hm.cluster.config.DeadNodeTimeout {
				hm.cluster.nodes.updateState(node.ID, NodeDead, nil)
				hm.cluster.config.Logger.Debugf("gossip: Marked suspect node as dead: %s", node.ID.String())
			}
		}

	case DeadNodeRetry:
		if success {
			// Dead node came back to life!
			hm.cluster.nodes.updateState(node.ID, NodeAlive, nil)
			node.updateLastActivity()
			hm.cluster.config.Logger.Infof("gossip: Dead node recovered: %s", node.ID.String())
		}
		// If still dead, just leave it as dead - we'll retry again later
	}
}

func (hm *HealthMonitor) pingNode(node *Node) bool {
	node.Address().Clear() // Force re-resolution

	pingMessage := &pingMessage{
		SenderID:      hm.cluster.localNode.ID,
		AdvertiseAddr: hm.cluster.localNode.advertiseAddr,
	}

	pongMessage := &pongMessage{}
	err := hm.cluster.sendToWithResponse(node, pingMsg, pingMessage, pongMessage)
	if err != nil {
		return false
	}

	// If we got a response, update our view of the node's address if needed
	if pongMessage.AdvertiseAddr != "" && pongMessage.AdvertiseAddr != node.advertiseAddr {
		node.advertiseAddr = pongMessage.AdvertiseAddr
		node.Address().Clear() // Force re-resolution
		hm.cluster.config.Logger.Tracef("gossip: Updated node address: %s -> %s", node.ID.String(), pongMessage.AdvertiseAddr)
	}

	return err == nil
}
