package sharder

import (
	"crypto/sha256"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/errors"
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
		return 0, errors.WithStack(err)
	}
	index := hash % uint32(len(shardIDs))
	return shardIDs[index], nil
}

func Hash(key []byte) (uint32, error) {
	// TODO we use a crypto hash to get a good distribution but there could be a better way. This might be slow.
	// I found that just using a non crypto hash like fnv resulted in very poorly distributed shards
	hasher := sha256.New()
	hasher.Write(key)
	res := hasher.Sum(nil)
	h2 := fnv.New32()
	if _, err := h2.Write(res); err != nil {
		return 0, errors.WithStack(err)
	}
	return h2.Sum32(), nil
}

func (s *Sharder) getShardIDs() []uint64 {
	return s.shardIDs.Load().([]uint64)
}

func (s *Sharder) setShardIDs(shardIds []uint64) {
	s.shardIDs.Store(shardIds)
}

func (s *Sharder) loadAllShards() {
	allShards := s.cluster.GetAllShardIDs()
	s.setShardIDs(allShards)
	return
}

func (s *Sharder) Start() error {
	s.startStopLock.Lock()
	defer s.startStopLock.Unlock()
	if s.started {
		return nil
	}
	s.loadAllShards()
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
