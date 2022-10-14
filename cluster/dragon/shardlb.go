package dragon

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

type NodeAllocation struct {
	NodeID uint64
	Score  int
}

type ShardLeaderBalancer interface {
	// Balance returns a set of leader swaps that balance the cluster.
	//
	// leaders is a map of shardID -> nodeID
	// nodeAllocations is a map of shardID -> [](nodeID, score)
	// swaps is a map of shardID -> nodeID
	Balance(leaders map[uint64]uint64, nodeAllocations map[uint64][]NodeAllocation) (swaps map[uint64]uint64)
}

type NoopShardLB struct{}

func (NoopShardLB) Balance(map[uint64]uint64, map[uint64][]NodeAllocation) map[uint64]uint64 {
	return nil
}

func NewStaticShardLB(nodeCount uint64) StaticShardLB {
	return StaticShardLB{nodeCount: nodeCount}
}

type StaticShardLB struct {
	nodeCount uint64
}

func (s StaticShardLB) Balance(leaders map[uint64]uint64, nodeAllocations map[uint64][]NodeAllocation) map[uint64]uint64 {
	swaps := make(map[uint64]uint64, 0)
	for shardID := range leaders {
		swaps[shardID] = shardID % s.nodeCount
	}
	return swaps
}

func (p *procManager) balanceShardLeaders() {
	swaps := p.computeShardLBSwaps()
	logSwaps("computed leader swaps", swaps)
	ownSwaps := p.filterOwnSwaps(swaps)
	logSwaps("applying leader swaps", ownSwaps)
	p.applySwaps(ownSwaps)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.scheduleShardLeaderBalancer()
}

func (p *procManager) computeShardLBSwaps() map[uint64]uint64 {
	leaders := p.getLeadersMap()
	nodeAllocations := make(map[uint64][]NodeAllocation, len(leaders))
	for shardID, leaderNodeID := range leaders {
		shardNodeIDs := p.dragon.shardAllocs[shardID]
		shardAlloc := make([]NodeAllocation, 0, len(shardNodeIDs))
		for _, nodeID := range shardNodeIDs {
			score := 0 // hard-code follower score to zero
			if uint64(nodeID) == leaderNodeID {
				score = 1 // hard-code leader score to one
			}
			shardAlloc = append(shardAlloc, NodeAllocation{
				NodeID: uint64(nodeID),
				Score:  score,
			})
		}
		nodeAllocations[shardID] = shardAlloc
	}
	return p.shardLB.Balance(leaders, nodeAllocations)
}

func logSwaps(msg string, swaps map[uint64]uint64) {
	if len(swaps) == 0 {
		return
	}
	s := make([]string, 0, len(swaps))
	for shardID, newLeaderNodeID := range swaps {
		s = append(s, fmt.Sprintf("%d->%d", shardID, newLeaderNodeID))
	}
	log.Infof("%s: %s", msg, strings.Join(s, ", "))
}

func (p *procManager) filterOwnSwaps(swaps map[uint64]uint64) map[uint64]uint64 {
	leadersMap := p.getLeadersMap()
	ownSwaps := make(map[uint64]uint64)
	for shardID, newLeaderNodeID := range swaps {
		currentLeaderNodeID := leadersMap[shardID]
		if currentLeaderNodeID == newLeaderNodeID {
			continue // skip if the new leader matches the current leader
		}
		for _, nodeID := range p.dragon.shardAllocs[shardID] {
			if uint64(nodeID) == p.nodeID {
				ownSwaps[shardID] = newLeaderNodeID
			}
		}
	}
	return ownSwaps
}

func (p *procManager) applySwaps(swaps map[uint64]uint64) {
	if !p.isDragonStarted() {
		return
	}
	for shardID, newLeaderNodeID := range swaps {
		dragonNodeID := newLeaderNodeID + 1
		if err := p.dragon.nh.RequestLeaderTransfer(shardID, dragonNodeID); err != nil {
			log.Warnf("leader swap unsuccessful: %v", err)
		}
	}
}

func (p *procManager) isDragonStarted() bool {
	p.dragon.lock.Lock()
	defer p.dragon.lock.Unlock()
	return p.dragon.started
}
