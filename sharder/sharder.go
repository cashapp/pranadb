package sharder

import (
	"github.com/squareup/pranadb/common"
	"hash/fnv"
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
		return s.computeHashShard(key, shardIDs)
	}
	panic("unsupported")
}

func (s *Sharder) computeHashShard(key []byte, shardIDs []uint64) (uint64, error) {
	hash, err := Hash(key)
	if err != nil {
		return 0, err
	}
	index := hash % uint32(len(shardIDs))
	return shardIDs[index], nil
}

func Hash(key []byte) (uint32, error) {
	// TODO consistent hashing when the cluster is not fixed size
	h1 := fnv.New64a()
	_, err := h1.Write(key)
	if err != nil {
		return 0, err
	}
	v1 := h1.Sum64()
	b := common.AppendUint64ToBufferLE(nil, v1)
	// hash it again to get a good distribution - I found a single hash can result in poorly distributed
	// values when the input was incrementing
	// TODO maybe find a better hash algo - maybe use a crypto hash, but.. performance?
	h2 := fnv.New32()
	_, err = h2.Write(b)
	if err != nil {
		return 0, err
	}
	return h2.Sum32(), nil
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
