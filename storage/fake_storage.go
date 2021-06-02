package storage

import (
	"fmt"
	"sync"
)

type FakeStorage struct {
	data map[uint64]partition
	mu sync.Mutex
}

type partition struct {
	data map[string][]byte
}

func (f *FakeStorage) writeBatch(partitionID uint64, batch *WriteBatch, localLeader bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.getPartitionOrFail(partitionID)
	if err != nil {
		return err
	}
	for _, kvPair := range batch.puts {
		p.data[string(kvPair.Key)] = kvPair.Value
	}
	for _, key := range batch.deletes {
		delete(p.data, string(key))
	}
	return nil
}

func (f *FakeStorage) installExecutors(partitionID uint64, plan *ExecutorPlan) {
	panic("implement me")
}

func (f *FakeStorage) get(partitionID uint64, key []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.getPartitionOrFail(partitionID)
	if err != nil {
		return nil, err
	}
	value, ok := p.data[string(key)]
	if !ok {
		return nil, nil
	} else {
		return value, nil
	}
}

func (f * FakeStorage) scan(partitionID uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([][]byte, error) {
	panic("implement me")
}


func (f *FakeStorage) getPartitionOrFail(partitionID uint64) (partition, error) {
	p, ok := f.data[partitionID]
	if !ok {
		return partition{}, fmt.Errorf("invalid partitionID %d", partitionID)
	} else {
		return p, nil
	}
}

