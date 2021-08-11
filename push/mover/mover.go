package mover

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"sync"
)

type RawRowHandler interface {
	HandleRawRows(rawRows map[uint64][][]byte, batch *cluster.WriteBatch) error
}

type Mover struct {
	cluster          cluster.Cluster
	lock             sync.RWMutex
	forwardSequences map[uint64]uint64
}

func NewMover(cluster cluster.Cluster) *Mover {
	return &Mover{
		cluster:          cluster,
		forwardSequences: make(map[uint64]uint64),
	}
}

func (m *Mover) QueueForRemoteSend(remoteShardID uint64, row *common.Row, localShardID uint64, remoteConsumerID uint64, colTypes []common.ColumnType, batch *cluster.WriteBatch) error {
	sequence, err := m.nextForwardSequence(localShardID)
	if err != nil {
		return err
	}

	queueKeyBytes := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 40)
	queueKeyBytes = common.AppendUint64ToBufferBE(queueKeyBytes, remoteShardID)
	queueKeyBytes = common.AppendUint64ToBufferBE(queueKeyBytes, sequence)
	queueKeyBytes = common.AppendUint64ToBufferBE(queueKeyBytes, remoteConsumerID)

	valueBuff := make([]byte, 0, 32)
	valueBuff, err = common.EncodeRow(row, colTypes, valueBuff)
	if err != nil {
		return err
	}
	batch.AddPut(queueKeyBytes, valueBuff)
	sequence++
	return m.updateNextForwardSequence(localShardID, sequence, batch)
}

// TransferData TODO instead of reading from storage, we can pass rows from QueueForRemoteSend to here via
// a channel - this will avoid scan of storage
func (m *Mover) TransferData(localShardID uint64, del bool) error {
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID+1, localShardID, 16)

	kvPairs, err := m.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return err
	}

	var batches []*forwardBatch
	var batch *forwardBatch
	var remoteShardID uint64
	first := true
	for _, kvPair := range kvPairs {
		key := kvPair.Key
		currRemoteShardID, _ := common.ReadUint64FromBufferBE(key, 16)
		if first || remoteShardID != currRemoteShardID {
			addBatch := cluster.NewWriteBatch(currRemoteShardID, true)
			deleteBatch := cluster.NewWriteBatch(localShardID, false)
			batch = &forwardBatch{
				addBatch:    addBatch,
				deleteBatch: deleteBatch,
			}
			batches = append(batches, batch)
			remoteShardID = currRemoteShardID
			first = false
		}

		remoteKey := table.EncodeTableKeyPrefix(common.ReceiverTableID, remoteShardID, 40)
		remoteKey = common.AppendUint64ToBufferBE(remoteKey, localShardID)

		// seq|remote_consumer_id are the last 16 bytes
		pos := len(key) - 16
		remoteKey = append(remoteKey, key[pos:]...)

		batch.addBatch.AddPut(remoteKey, kvPair.Value)
		batch.deleteBatch.AddDelete(key)
	}

	// We send these in parallel for better performance
	lb := len(batches)
	chs := make([]chan error, lb)
	for i, fBatch := range batches {
		chs[i] = m.sendRemoteBatch(fBatch, del)
	}
	for i := 0; i < lb; i++ {
		err, ok := <-chs[i]
		if !ok {
			panic("channel closed")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Mover) sendRemoteBatch(fBatch *forwardBatch, del bool) chan error {
	ch := make(chan error, 1)
	go func() {
		// Write to the remote shard
		err := m.cluster.WriteBatch(fBatch.addBatch)
		if err != nil {
			ch <- err
			return
		}
		if del {
			// Delete locally
			err = m.cluster.WriteBatch(fBatch.deleteBatch)
			if err != nil {
				ch <- err
				return
			}
		}
		ch <- nil
	}()
	return ch
}

type forwardBatch struct {
	addBatch    *cluster.WriteBatch
	deleteBatch *cluster.WriteBatch
}

func (m *Mover) HandleReceivedRows(receivingShardID uint64, rawRowHandler RawRowHandler) error {
	batch := cluster.NewWriteBatch(receivingShardID, false)
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID, receivingShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID+1, receivingShardID, 16)

	kvPairs, err := m.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return err
	}
	remoteConsumerRows := make(map[uint64][][]byte)
	receivingSequences := make(map[uint64]uint64)
	for _, kvPair := range kvPairs {
		sendingShardID, _ := common.ReadUint64FromBufferBE(kvPair.Key, 16)
		lastReceivedSeq, ok := receivingSequences[sendingShardID]
		if !ok {
			lastReceivedSeq, err = m.lastReceivingSequence(receivingShardID, sendingShardID)
			if err != nil {
				return err
			}
		}

		receivedSeq, _ := common.ReadUint64FromBufferBE(kvPair.Key, 24)
		remoteConsumerID, _ := common.ReadUint64FromBufferBE(kvPair.Key, 32)
		if receivedSeq > lastReceivedSeq {
			// We only handle rows which we haven't seen before - it's possible the forwarder
			// might forwarder the same row more than once after failure
			// They get deleted
			rows, ok := remoteConsumerRows[remoteConsumerID]
			if !ok {
				rows = make([][]byte, 0)
			}
			rows = append(rows, kvPair.Value)
			remoteConsumerRows[remoteConsumerID] = rows
			lastReceivedSeq = receivedSeq
			receivingSequences[sendingShardID] = lastReceivedSeq
		}
		batch.AddDelete(kvPair.Key)
	}
	if len(remoteConsumerRows) > 0 {
		err = rawRowHandler.HandleRawRows(remoteConsumerRows, batch)
		if err != nil {
			return err
		}
	}
	for sendingShardID, lastReceivedSequence := range receivingSequences {
		err = m.updateLastReceivingSequence(receivingShardID, sendingShardID, lastReceivedSequence, batch)
		if err != nil {
			return err
		}
	}
	return m.cluster.WriteBatch(batch)
}

// TODO consider caching sequences in memory to avoid reading from storage each time
// Return the next forward sequence value
func (m *Mover) nextForwardSequence(localShardID uint64) (uint64, error) {

	// TODO Rlocks don't scale well over multiple cores - we can remove this one by caching
	// the last sequence on the scheduler and passing it in the context
	m.lock.RLock()
	defer m.lock.RUnlock()

	lastSeq, ok := m.forwardSequences[localShardID]
	if !ok {
		seqKey := m.genForwardSequenceKey(localShardID)
		seqBytes, err := m.cluster.LocalGet(seqKey)
		if err != nil {
			return 0, err
		}
		if seqBytes == nil {
			return 1, nil
		}
		lastSeq, _ = common.ReadUint64FromBufferLE(seqBytes, 0)
		m.forwardSequences[localShardID] = lastSeq
	}

	return lastSeq, nil
}

func (m *Mover) updateNextForwardSequence(localShardID uint64, sequence uint64, batch *cluster.WriteBatch) error {
	seqKey := m.genForwardSequenceKey(localShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLE(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	// TODO remove this lock!
	m.lock.Lock()
	defer m.lock.Unlock()
	m.forwardSequences[localShardID] = sequence
	return nil
}

// TODO consider caching sequences in memory to avoid reading from storage each time
func (m *Mover) lastReceivingSequence(receivingShardID uint64, sendingShardID uint64) (uint64, error) {
	seqKey := m.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqBytes, err := m.cluster.LocalGet(seqKey)
	if err != nil {
		return 0, err
	}
	if seqBytes == nil {
		return 0, nil
	}
	res, _ := common.ReadUint64FromBufferLE(seqBytes, 0)
	return res, nil
}

func (m *Mover) updateLastReceivingSequence(receivingShardID uint64, sendingShardID uint64, sequence uint64, batch *cluster.WriteBatch) error {
	seqKey := m.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLE(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	return nil
}

func (m *Mover) genForwardSequenceKey(localShardID uint64) []byte {
	return table.EncodeTableKeyPrefix(common.ForwarderSequenceTableID, localShardID, 16)
}

func (m *Mover) genReceivingSequenceKey(receivingShardID uint64, sendingShardID uint64) []byte {
	seqKey := table.EncodeTableKeyPrefix(common.ReceiverSequenceTableID, receivingShardID, 24)
	return common.AppendUint64ToBufferBE(seqKey, sendingShardID)
}
