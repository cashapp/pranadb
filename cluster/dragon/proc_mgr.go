package dragon

import (
	"github.com/lni/dragonboat/v3/raftio"
	log "github.com/sirupsen/logrus"
	"sync"
)

func newProcManager(d *Dragon) *procManager {
	return &procManager{
		nodeID: d.cnf.NodeID,
		dragon: d,
		leaderShards: map[uint64]struct{}{},
		setLeaderChannel: make(chan uint64, 10),
	}
}

type procManager struct {
	nodeID int
	dragon *Dragon
	leaderShards map[uint64]struct{} // do we need this
	setLeaderChannel chan uint64
	closeWG sync.WaitGroup
	lock sync.Mutex
}

func (p *procManager) LeaderUpdated(info raftio.LeaderInfo) {
	p.lock.Lock()
	defer p.lock.Unlock()

	log.Infof("node %d received leader updated cluster id %d node id %d term %d leader id %d",
		p.nodeID, info.ClusterID, info.NodeID, info.Term, info.LeaderID)
	if info.NodeID != uint64(p.nodeID) + 1 {
		panic("received leader info on wrong node")
	}
	// When a later node joins, it won't receive info for membership changes before it joined - but it will
	// know if becomes the leader for a node, or some other node takes over leadership
	if info.LeaderID == uint64(p.nodeID) + 1 {
		p.leaderShards[info.ClusterID] = struct{}{}
		p.setLeaderChannel <- info.ClusterID
	} else {
		delete(p.leaderShards, info.ClusterID)
	}
}

/*
We won't receive leader updates here for shards which don't have a local replica.
So, periodically we must ping all other nodes, and ask for their leader maps, then we add it to this map, we must get the term
too and not overwrite if it's an earlier term.
Then we can maintain a map of shard_id -> node id for the whole cluster

When nodes call a leader, e.g. forwarding to a processor, it's possible the processor hasn't been started yet, or is old, in either
case an error will be returned and the request can be retried after getting the leader again and retrying
 */

func (p *procManager) getLeaderNode(shardID uint64) int {
	// TODO
	return 0
}

func (p *procManager) waitForShardsToBeReady(shardIDs []uint64) {
	// TODO
	// wait until all nodes have leaders-  take the place of ping lookup
}

/*
Timeout policy

In the case of loss of nodes where quorum is maintained, should never have to wait much longer than election interval.
In the case quorum is lost an node needs to be brought back up, need to wait as long as node restart interval.

So.... we should tune up timeouts such that scheduler fail doesn't get called under these circumstances. Scheduler fail should never
be called if a write fails because leader not available. Instead it should be retried after delay, unless the scheduler is stopped.
Scheduler fail should only be called for unexpected errors not due to node failure.

However, queueing of forward writes on processor - these should timeout in good time, and the source should be stopped temporarily with retry.

 */

func (p *procManager) Start() {
	p.closeWG.Add(1)
	go p.setLeaderLoop()
}

func (p *procManager) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	close(p.setLeaderChannel)
	p.closeWG.Wait()
}

func (p *procManager) setLeaderLoop() {
	for {
		shardID, ok := <- p.setLeaderChannel
		if !ok {
			break
		}
		// Now send a propose to the shard saying we're new leader
		if err := p.dragon.setLeader(shardID); err != nil {
			log.Errorf("failed to set leader %+v", err)
			break
		}
	}
	p.closeWG.Done()
}

