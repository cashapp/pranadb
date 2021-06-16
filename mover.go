package pranadb

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	"github.com/squareup/pranadb/storage"
	"unsafe"
)

const (
	ForwarderTableID         = 1
	ForwarderSequenceTableID = 2
	ReceiverTableID          = 3
	ReceiverSequenceTableID  = 4
)

type RemoteRowsHandler interface {
	HandleRemoteRows(rows *common.PushRows, ctx *exec.ExecutionContext) error
}

type ReceiverHandler interface {
	HandleRows(entityValues map[uint64][][]byte, batch *storage.WriteBatch) error
}

func NewMover(store storage.Storage, receiveHandler ReceiverHandler, sharder Sharder) *Mover {
	return &Mover{
		store:   store,
		handler: receiveHandler,
		sharder: sharder,
	}
}

type Mover struct {
	store   storage.Storage
	handler ReceiverHandler
	sharder Sharder
}

func (m *Mover) QueueForRemoteSend(key []byte, row *common.PullRow, localShardID uint64, entityID uint64, colTypes []common.ColumnType, batch *storage.WriteBatch) error {
	remoteShardID, err := m.sharder.CalculateShard(key)
	if err != nil {
		return err
	}
	sequence, err := m.nextForwardSequence(localShardID)
	if err != nil {
		return err
	}
	queueKeyBytes := encodeLocalKey(localShardID, remoteShardID, entityID, sequence)
	valueBuff := make([]byte, 0, 32)
	valueBuff, err = common.EncodeRow(row, colTypes, valueBuff)
	if err != nil {
		return err
	}
	kvPair := storage.KVPair{
		Key:   queueKeyBytes,
		Value: valueBuff,
	}
	batch.AddPut(kvPair)
	sequence++
	return m.updateNextForwardSequence(localShardID, sequence, batch)
}

// TODO instead of reading from storage, we can pass rows from QueueForRemoteSend to here via
// a channel - this will avoid scan of storage
func (m *Mover) pollForForwards(localShardID uint64) error {
	keyStartPrefix := make([]byte, 0, 16)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, localShardID)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, ForwarderTableID)

	// TODO make limit configurable
	kvPairs, err := m.store.Scan(localShardID, keyStartPrefix, nil, 100)
	if err != nil {
		return err
	}

	var addBatches []*storage.WriteBatch
	var deleteBatches []*storage.WriteBatch
	var remoteBatch *storage.WriteBatch
	var deleteBatch *storage.WriteBatch
	var remoteShardID uint64
	var first = true
	for _, kvPair := range kvPairs {
		key := kvPair.Key
		// Key structure is
		// shard_id|forwarder_table_id|remote_shard_id|seq|entity_id
		currRemoteShardID := *(*uint64)(unsafe.Pointer(&key[16]))
		if first || remoteShardID != currRemoteShardID {
			remoteBatch = storage.NewWriteBatch(remoteShardID)
			addBatches = append(addBatches, remoteBatch)
			deleteBatch = storage.NewWriteBatch(remoteShardID)
			deleteBatches = append(deleteBatches, deleteBatch)
			remoteShardID = currRemoteShardID
			first = false
		}

		// Required key is
		// remote_shard_id|receiver_table_id|sending_shard_id|seq_number|entity_id

		remoteKey := make([]byte, 32)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, remoteShardID)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, ReceiverTableID)
		remoteKey = common.AppendUint64ToBufferLittleEndian(remoteKey, localShardID)

		// seq|entity_id are the last 16 bytes
		pos := len(key) - 16
		remoteKey = append(remoteKey, key[pos:]...)
		remoteKVPair := storage.KVPair{
			Key:   remoteKey,
			Value: kvPair.Value,
		}
		remoteBatch.AddPut(remoteKVPair)
		deleteBatch.AddDelete(key)
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

func (m *Mover) PollForReceives(receivingShardID uint64, batch *storage.WriteBatch) error {
	keyStartPrefix := make([]byte, 0, 16)
	// key is:
	// remote_shard_id|receiver_table_id|sending_shard_id|seq_number|entity_id
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, receivingShardID)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, ReceiverTableID)

	// TODO make limit configurable
	kvPairs, err := m.store.Scan(receivingShardID, keyStartPrefix, nil, 100)
	if err != nil {
		return err
	}
	entityValues := make(map[uint64][][]byte)

	receivingSequences := make(map[uint64]uint64)

	for _, kvPair := range kvPairs {
		sendingShardID := *(*uint64)(unsafe.Pointer(&kvPair.Key[16]))

		lastReceivedSeq, ok := receivingSequences[sendingShardID]
		if !ok {
			lastReceivedSeq, err = m.nextReceivingSequence(receivingShardID, sendingShardID)
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
				rows := make([][]byte, 0)
				entityValues[entityID] = rows
			}
			rows = append(rows, kvPair.Value)
		}
		batch.AddDelete(kvPair.Key)
		lastReceivedSeq = receivedSeq
		receivingSequences[sendingShardID] = lastReceivedSeq
	}

	err = m.handler.HandleRows(entityValues, batch)
	if err != nil {
		for sendingShardID, lastReceivedSequence := range receivingSequences {
			err = m.updateNextReceivingSequence(receivingShardID, sendingShardID, lastReceivedSequence, batch)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func encodeLocalKey(localShardID uint64, remoteShardID uint64, entityID uint64, sequence uint64) []byte {
	keyBuff := make([]byte, 0, 40)
	// Local key structure is:
	// shard_id|forwarder_table_id|remote_shard_id|sequence_number|entity_id
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, localShardID)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, ForwarderTableID)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, remoteShardID)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, sequence)
	return common.AppendUint64ToBufferLittleEndian(keyBuff, entityID)
}

// TODO consider caching sequences in memory to avoid reading from storage each time
func (m *Mover) nextForwardSequence(localShardID uint64) (uint64, error) {
	seqKey := m.genForwardSequenceKey(localShardID)
	seqBytes, err := m.store.Get(localShardID, seqKey, true)
	if err != nil {
		return 0, err
	}
	return *(*uint64)(unsafe.Pointer(&seqBytes)), nil
}

func (m *Mover) updateNextForwardSequence(localShardID uint64, sequence uint64, batch *storage.WriteBatch) error {
	seqKey := m.genForwardSequenceKey(localShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, sequence)
	seqKvPair := storage.KVPair{
		Key:   seqKey,
		Value: seqValueBytes,
	}
	batch.AddPut(seqKvPair)
	return nil
}

// TODO consider caching sequences in memory to avoid reading from storage each time
func (m *Mover) nextReceivingSequence(receivingShardID uint64, sendingShardID uint64) (uint64, error) {
	seqKey := m.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqBytes, err := m.store.Get(receivingShardID, seqKey, true)
	if err != nil {
		return 0, err
	}
	return *(*uint64)(unsafe.Pointer(&seqBytes)), nil
}

func (m *Mover) updateNextReceivingSequence(receivingShardID uint64, sendingShardID uint64, sequence uint64, batch *storage.WriteBatch) error {
	seqKey := m.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, sequence)
	seqKvPair := storage.KVPair{
		Key:   seqKey,
		Value: seqValueBytes,
	}
	batch.AddPut(seqKvPair)
	return nil
}

func (m *Mover) genForwardSequenceKey(localShardID uint64) []byte {
	seqKey := make([]byte, 0, 24)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, localShardID)
	return common.AppendUint64ToBufferLittleEndian(seqKey, ForwarderSequenceTableID)
}

func (m *Mover) genReceivingSequenceKey(receivingShardID uint64, sendingShardID uint64) []byte {
	seqKey := make([]byte, 0, 24)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, receivingShardID)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, ReceiverSequenceTableID)
	return common.AppendUint64ToBufferLittleEndian(seqKey, sendingShardID)
}
