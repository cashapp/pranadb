package sharder

import (
	"sync"
	"sync/atomic"

	"github.com/squareup/pranadb/cluster"
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
	}
	panic("unsupported")
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

func (s *Sharder) loadAllShards() error {
	clusterInfo, err := s.cluster.GetClusterInfo()
	if err != nil {
		return err
	}
	var shardIDs []uint64
	// TODO is this really necessary? If shard ids form a contiguous sequence then it's not
	for _, nodeInfo := range clusterInfo.NodeInfos {
		for _, leader := range nodeInfo.Leaders {
			shardIDs = append(shardIDs, leader)
		}
	}
	s.setShardIDs(shardIDs)
	return nil
}

func (s *Sharder) Start() error {
	s.startStopLock.Lock()
	defer s.startStopLock.Unlock()
	if s.started {
		return nil
	}
	if err := s.loadAllShards(); err != nil {
		return err
	}
	s.started = true
	return nil
}

func (s *Sharder) Stop() error {
	s.startStopLock.Lock()
	defer s.startStopLock.Unlock()
	if !s.started {
		return nil
	}
	s.started = false
	return nil
}
