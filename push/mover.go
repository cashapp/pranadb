package push

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
	"log"
	"unsafe"
)

const (
	ForwarderTableID         = 1
	ForwarderSequenceTableID = 2
	ReceiverTableID          = 3
	ReceiverSequenceTableID  = 4
)

func newMover(store storage.Storage, receiveHandler receiverHandler, sharder common.Sharder) *mover {
	return &mover{
		store:   store,
		handler: receiveHandler,
		sharder: sharder,
	}
}

type mover struct {
	store   storage.Storage
	handler receiverHandler
	sharder common.Sharder
}

func (m *mover) QueueForRemoteSend(key []byte, remoteShardID uint64, row *common.Row, localShardID uint64, entityID uint64, colTypes []common.ColumnType, batch *storage.WriteBatch) error {
	log.Printf("Queueing row for remote send from shard %d to shard %d for entityid %d key is %v", localShardID, remoteShardID, entityID, key)
	sequence, err := m.lastForwardSequence(localShardID)
	if err != nil {
		return err
	}
	sequence++

	queueKeyBytes := make([]byte, 0, 40)

	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, ForwarderTableID)
	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, localShardID)
	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, remoteShardID)
	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, sequence)
	queueKeyBytes = common.AppendUint64ToBufferLittleEndian(queueKeyBytes, entityID)

	valueBuff := make([]byte, 0, 32)
	valueBuff, err = common.EncodeRow(row, colTypes, valueBuff)
	if err != nil {
		return err
	}
	batch.AddPut(queueKeyBytes, valueBuff)
	sequence++
	return m.updateLastForwardSequence(localShardID, sequence, batch)
}

// TODO instead of reading from storage, we can pass rows from QueueForRemoteSend to here via
// a channel - this will avoid scan of storage
func (m *mover) PollForForwards(localShardID uint64) error {
	log.Printf("Polling for forwards on shard %d", localShardID)
	keyStartPrefix := make([]byte, 0, 16)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, ForwarderTableID)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, localShardID)

	// TODO make limit configurable
	kvPairs, err := m.store.Scan(keyStartPrefix, keyStartPrefix, 100)
	if err != nil {
		return err
	}
	// TODO if num rows returned = limit async schedule another batch

	var addBatches []*storage.WriteBatch
	var deleteBatches []*storage.WriteBatch
	var remoteBatch *storage.WriteBatch
	var deleteBatch *storage.WriteBatch
	var remoteShardID uint64
	first := true
	log.Printf("There are %d rows to forward", len(kvPairs))
	for _, kvPair := range kvPairs {
		key := kvPair.Key
		log.Printf("Key is %v", key)

		// Key structure is
		// shard_id|forwarder_table_id|remote_shard_id|seq|entity_id
		currRemoteShardID := *(*uint64)(unsafe.Pointer(&key[16]))
		log.Printf("Curr remote shard id is %d", currRemoteShardID)
		if first || remoteShardID != currRemoteShardID {
			remoteBatch = storage.NewWriteBatch(currRemoteShardID)
			addBatches = append(addBatches, remoteBatch)
			deleteBatch = storage.NewWriteBatch(currRemoteShardID)
			deleteBatches = append(deleteBatches, deleteBatch)
			remoteShardID = currRemoteShardID
			first = false
		}
		log.Printf("remote shard id is %d", remoteShardID)

		remoteKey := make([]byte, 0, 40)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, ReceiverTableID)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, remoteShardID)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, localShardID)

		// seq|entity_id are the last 16 bytes
		pos := len(key) - 16
		remoteKey = append(remoteKey, key[pos:]...)
		remoteBatch.AddPut(remoteKey, kvPair.Value)
		deleteBatch.AddDelete(key)

		log.Printf("Forwarding row to shard %d with key %v batch shard id is %d", currRemoteShardID, remoteKey, remoteBatch.ShardID)
	}

	// TODO  Different addBatches can be executed concurrently
	for _, batch := range addBatches {
		// Write to the remote shard
		err := m.store.WriteBatch(batch, false)
		if err != nil {
			return err
		}
		// Delete locally
		err = m.store.WriteBatch(deleteBatch, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *mover) HandleReceivedRows(receivingShardID uint64, batch *storage.WriteBatch) error {
	log.Printf("In HandleReceivedRows for shard id %d", receivingShardID)
	keyStartPrefix := make([]byte, 0, 16)

	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, ReceiverTableID)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, receivingShardID)

	// TODO make limit configurable
	kvPairs, err := m.store.Scan(keyStartPrefix, keyStartPrefix, 100)
	log.Printf("Found %d received rows", len(kvPairs))
	if err != nil {
		return err
	}
	// TODO if num rows returned = limit async schedule another batch
	entityValues := make(map[uint64][][]byte)

	receivingSequences := make(map[uint64]uint64)

	for _, kvPair := range kvPairs {
		sendingShardID := *(*uint64)(unsafe.Pointer(&kvPair.Key[16]))

		log.Printf("Received key %v", kvPair.Key)

		lastReceivedSeq, ok := receivingSequences[sendingShardID]
		if !ok {
			lastReceivedSeq, err = m.lastReceivingSequence(receivingShardID, sendingShardID)
			if err != nil {
				return err
			}
		}

		receivedSeq := *(*uint64)(unsafe.Pointer(&kvPair.Key[24]))
		entityID := *(*uint64)(unsafe.Pointer(&kvPair.Key[32]))
		if receivedSeq > lastReceivedSeq {
			// We only handle rows which we haven't seen before - it's possible the forwarder
			// might forwarder the same row more than once after failure
			// They get deleted
			rows, ok := entityValues[entityID]
			if !ok {
				rows = make([][]byte, 0)
			}
			rows = append(rows, kvPair.Value)
			entityValues[entityID] = rows
		}
		batch.AddDelete(kvPair.Key)
		lastReceivedSeq = receivedSeq
		receivingSequences[sendingShardID] = lastReceivedSeq
	}

	err = m.handler.HandleRemoteRows(entityValues, batch)
	if err != nil {
		for sendingShardID, lastReceivedSequence := range receivingSequences {
			err = m.updateLastReceivingSequence(receivingShardID, sendingShardID, lastReceivedSequence, batch)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// TODO consider caching sequences in memory to avoid reading from storage each time
func (m *mover) lastForwardSequence(localShardID uint64) (uint64, error) {
	seqKey := m.genForwardSequenceKey(localShardID)
	seqBytes, err := m.store.Get(localShardID, seqKey, true)
	if err != nil {
		return 0, err
	}
	if seqBytes == nil {
		return 0, nil
	}
	return *(*uint64)(unsafe.Pointer(&seqBytes)), nil
}

func (m *mover) updateLastForwardSequence(localShardID uint64, sequence uint64, batch *storage.WriteBatch) error {
	seqKey := m.genForwardSequenceKey(localShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	return nil
}

// TODO consider caching sequences in memory to avoid reading from storage each time
func (m *mover) lastReceivingSequence(receivingShardID uint64, sendingShardID uint64) (uint64, error) {
	seqKey := m.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqBytes, err := m.store.Get(receivingShardID, seqKey, true)
	if err != nil {
		return 0, err
	}
	return *(*uint64)(unsafe.Pointer(&seqBytes)), nil
}

func (m *mover) updateLastReceivingSequence(receivingShardID uint64, sendingShardID uint64, sequence uint64, batch *storage.WriteBatch) error {
	seqKey := m.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	return nil
}

func (m *mover) genForwardSequenceKey(localShardID uint64) []byte {
	seqKey := make([]byte, 0, 24)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, ForwarderSequenceTableID)
	return common.AppendUint64ToBufferLittleEndian(seqKey, localShardID)
}

func (m *mover) genReceivingSequenceKey(receivingShardID uint64, sendingShardID uint64) []byte {
	seqKey := make([]byte, 0, 24)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, ReceiverSequenceTableID)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, receivingShardID)
	return common.AppendUint64ToBufferLittleEndian(seqKey, sendingShardID)
}
