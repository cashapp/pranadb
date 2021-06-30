package sharder

import (
	"github.com/squareup/pranadb/cluster"
	"sync"
	"sync/atomic"
)

type ShardType int

const (
	ShardTypeHash = iota + 1
	ShardTypeRange
)

type Sharder struct {
	shardIDs      atomic.Value
	cluster       cluster.Cluster
	started       bool
	startStopLock sync.Mutex
}

func NewSharder(cluster cluster.Cluster) *Sharder {
	return &Sharder{
		cluster: cluster,
	}
}

func (s *Sharder) CalculateShard(shardType ShardType, key []byte) (uint64, error) {
	if shardType == ShardTypeHash {
		return s.computeHashShard(key), nil
	} else {
		panic("unsupported")
	}
}

func (s *Sharder) computeHashShard(key []byte) uint64 {
	shardIDs := s.getShardIDs()
	hash := hash(key)
	index := hash % uint32(len(shardIDs))
	return shardIDs[index]
}

func hash(key []byte) uint32 {
	// TODO consistent hashing
	hash := uint32(31)
	for _, b := range key {
		hash = 31*hash + uint32(b)
	}
	return hash
}

func (s *Sharder) getShardIDs() []uint64 {
	return s.shardIDs.Load().([]uint64)
}

func (s *Sharder) setShardIDs(shardIds []uint64) {
	s.shardIDs.Store(shardIds)
}

func (p *Sharder) loadAllShards() error {
	clusterInfo, err := p.cluster.GetClusterInfo()
	if err != nil {
		return err
	}
	var shardIDs []uint64
	for _, nodeInfo := range clusterInfo.NodeInfos {
		for _, leader := range nodeInfo.Leaders {
			shardIDs = append(shardIDs, leader)
		}
	}
	p.setShardIDs(shardIDs)
	return nil
}

func (p *Sharder) Start() error {
	p.startStopLock.Lock()
	defer p.startStopLock.Unlock()
	if p.started {
		return nil
	}
	err := p.loadAllShards()
	if err != nil {
		return nil
	}
	p.started = true
	return nil
}

func (p *Sharder) Stop() error {
	p.startStopLock.Lock()
	defer p.startStopLock.Unlock()
	if !p.started {
		return nil
	}
	p.started = false
	return nil
}
