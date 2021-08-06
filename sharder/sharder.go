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
	shardIDs := s.getShardIDs()
	return s.CalculateShardWithShardIDs(shardType, key, shardIDs)
}

func (s *Sharder) CalculateShardWithShardIDs(shardType ShardType, key []byte, shardIDs []uint64) (uint64, error) {
	if shardType == ShardTypeHash {
		return s.computeHashShard(key, shardIDs), nil
	}
	panic("unsupported")
}

func (s *Sharder) computeHashShard(key []byte, shardIDs []uint64) uint64 {
	hash := hash(key)
	index := hash % uint32(len(shardIDs))
	return shardIDs[index]
}

func hash(key []byte) uint32 {
	// TODO consistent hashing when the cluster is not fixed size
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
	allShards := s.cluster.GetAllShardIDs()
	s.setShardIDs(allShards)
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
