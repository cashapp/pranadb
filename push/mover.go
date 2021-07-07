package push

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
)

// Don't use iota here as these must not change
const (
	ForwarderTableID         = 1
	ForwarderSequenceTableID = 2
	ReceiverTableID          = 3
	ReceiverSequenceTableID  = 4
)

func (p *PushEngine) QueueForRemoteSend(key []byte, remoteShardID uint64, row *common.Row, localShardID uint64, remoteConsumerID uint64, colTypes []common.ColumnType, batch *storage.WriteBatch) error {
	sequence, err := p.nextForwardSequence(localShardID)
	if err != nil {
		return err
	}

	queueKeyBytes := make([]byte, 0, 40)

	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, ForwarderTableID)
	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, localShardID)
	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, remoteShardID)
	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, sequence)
	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, remoteConsumerID)

	valueBuff := make([]byte, 0, 32)
	valueBuff, err = common.EncodeRow(row, colTypes, valueBuff)
	if err != nil {
		return err
	}
	batch.AddPut(queueKeyBytes, valueBuff)
	sequence++
	return p.updateNextForwardSequence(localShardID, sequence, batch)
}

// TODO instead of reading from storage, we can pass rows from QueueForRemoteSend to here via
// a channel - this will avoid scan of storage
func (p *PushEngine) transferData(localShardID uint64, delete bool) error {
	keyStartPrefix := make([]byte, 0, 16)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, ForwarderTableID)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, localShardID)

	// TODO make limit configurable
	kvPairs, err := p.storage.Scan(keyStartPrefix, keyStartPrefix, 100)
	if err != nil {
		return err
	}
	// TODO if num rows returned = limit async schedule another batch

	var batches []*forwardBatch
	var batch *forwardBatch
	var remoteShardID uint64
	first := true
	for _, kvPair := range kvPairs {
		key := kvPair.Key
		currRemoteShardID := common.ReadUint64FromBufferLittleEndian(key, 16)
		if first || remoteShardID != currRemoteShardID {
			addBatch := storage.NewWriteBatch(currRemoteShardID)
			deleteBatch := storage.NewWriteBatch(localShardID)
			batch = &forwardBatch{
				addBatch:    addBatch,
				deleteBatch: deleteBatch,
			}
			batches = append(batches, batch)
			remoteShardID = currRemoteShardID
			first = false
		}

		remoteKey := make([]byte, 0, 40)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, ReceiverTableID)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, remoteShardID)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, localShardID)

		// seq|remote_consumer_id are the last 16 bytes
		pos := len(key) - 16
		remoteKey = append(remoteKey, key[pos:]...)
		batch.addBatch.AddPut(remoteKey, kvPair.Value)
		batch.deleteBatch.AddDelete(key)
	}

	for _, fBatch := range batches {
		// Write to the remote shard
		err := p.storage.WriteBatch(fBatch.addBatch, false)
		if err != nil {
			return err
		}
		if delete {
			// Delete locally
			err = p.storage.WriteBatch(fBatch.deleteBatch, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type forwardBatch struct {
	addBatch    *storage.WriteBatch
	deleteBatch *storage.WriteBatch
}

func (p *PushEngine) handleReceivedRows(receivingShardID uint64, rawRowHandler RawRowHandler) error {
	batch := storage.NewWriteBatch(receivingShardID)
	keyStartPrefix := make([]byte, 0, 16)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, ReceiverTableID)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, receivingShardID)

	// TODO make limit configurable
	kvPairs, err := p.storage.Scan(keyStartPrefix, keyStartPrefix, 100)
	if err != nil {
		return err
	}
	// TODO if num rows returned = limit async schedule another batch
	remoteConsumerRows := make(map[uint64][][]byte)
	receivingSequences := make(map[uint64]uint64)
	for _, kvPair := range kvPairs {
		sendingShardID := common.ReadUint64FromBufferLittleEndian(kvPair.Key, 16)
		lastReceivedSeq, ok := receivingSequences[sendingShardID]
		if !ok {
			lastReceivedSeq, err = p.lastReceivingSequence(receivingShardID, sendingShardID)
			if err != nil {
				return err
			}
		}

		receivedSeq := common.ReadUint64FromBufferLittleEndian(kvPair.Key, 24)
		remoteConsumerID := common.ReadUint64FromBufferLittleEndian(kvPair.Key, 32)
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

	err = rawRowHandler.HandleRawRows(remoteConsumerRows, batch)
	if err != nil {
		return err
	}
	for sendingShardID, lastReceivedSequence := range receivingSequences {
		err = p.updateLastReceivingSequence(receivingShardID, sendingShardID, lastReceivedSequence, batch)
		if err != nil {
			return err
		}
	}
	return p.storage.WriteBatch(batch, true)
}

// TODO consider caching sequences in memory to avoid reading from storage each time
// Return the next forward sequence value
func (p *PushEngine) nextForwardSequence(localShardID uint64) (uint64, error) {

	// TODO Rlocks don't scale well over multiple cores - we can remove this one by caching
	// the last sequence on the scheduler and passing it in the context
	p.lock.RLock()
	defer p.lock.RUnlock()

	lastSeq, ok := p.forwardSequences[localShardID]
	if !ok {
		seqKey := p.genForwardSequenceKey(localShardID)
		seqBytes, err := p.storage.Get(localShardID, seqKey, true)
		if err != nil {
			return 0, err
		}
		if seqBytes == nil {
			return 1, nil
		}
		lastSeq = common.ReadUint64FromBufferLittleEndian(seqBytes, 0)
		p.forwardSequences[localShardID] = lastSeq
	}

	return lastSeq, nil
}

func (p *PushEngine) updateNextForwardSequence(localShardID uint64, sequence uint64, batch *storage.WriteBatch) error {
	seqKey := p.genForwardSequenceKey(localShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	p.lock.RLock()
	defer p.lock.RUnlock()
	p.forwardSequences[localShardID] = sequence
	return nil
}

// TODO consider caching sequences in memory to avoid reading from storage each time
func (p *PushEngine) lastReceivingSequence(receivingShardID uint64, sendingShardID uint64) (uint64, error) {
	seqKey := p.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqBytes, err := p.storage.Get(receivingShardID, seqKey, true)
	if err != nil {
		return 0, err
	}
	if seqBytes == nil {
		return 0, nil
	}
	return common.ReadUint64FromBufferLittleEndian(seqBytes, 0), nil
}

func (p *PushEngine) updateLastReceivingSequence(receivingShardID uint64, sendingShardID uint64, sequence uint64, batch *storage.WriteBatch) error {
	seqKey := p.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	return nil
}

func (p *PushEngine) genForwardSequenceKey(localShardID uint64) []byte {
	seqKey := make([]byte, 0, 16)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, ForwarderSequenceTableID)
	return common.AppendUint64ToBufferLittleEndian(seqKey, localShardID)
}

func (p *PushEngine) genReceivingSequenceKey(receivingShardID uint64, sendingShardID uint64) []byte {
	seqKey := make([]byte, 0, 24)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, ReceiverSequenceTableID)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, receivingShardID)
	return common.AppendUint64ToBufferLittleEndian(seqKey, sendingShardID)
}
