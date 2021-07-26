package push

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"log"
)

func (p *PushEngine) QueueForRemoteSend(key []byte, remoteShardID uint64, row *common.Row, localShardID uint64, remoteConsumerID uint64, colTypes []common.ColumnType, batch *cluster.WriteBatch) error {
	sequence, err := p.nextForwardSequence(localShardID)
	if err != nil {
		return err
	}

	log.Printf("Queueing data for transfer from shard %d on node %d to remote shard %d", localShardID, p.cluster.GetNodeID(), remoteShardID)

	queueKeyBytes := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 40)
	queueKeyBytes = common.AppendUint64ToBufferBigEndian(queueKeyBytes, remoteShardID)
	queueKeyBytes = common.AppendUint64ToBufferBigEndian(queueKeyBytes, sequence)
	queueKeyBytes = common.AppendUint64ToBufferBigEndian(queueKeyBytes, remoteConsumerID)

	log.Printf("Queued key %v", queueKeyBytes)

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
func (p *PushEngine) transferData(localShardID uint64, del bool) error {
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID+1, localShardID, 16)

	kvPairs, err := p.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return err
	}

	var batches []*forwardBatch
	var batch *forwardBatch
	var remoteShardID uint64
	first := true
	for _, kvPair := range kvPairs {
		key := kvPair.Key
		currRemoteShardID := common.ReadUint64FromBufferBigEndian(key, 16)
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
		remoteKey = common.AppendUint64ToBufferBigEndian(remoteKey, localShardID)

		// seq|remote_consumer_id are the last 16 bytes
		pos := len(key) - 16
		remoteKey = append(remoteKey, key[pos:]...)
		batch.addBatch.AddPut(remoteKey, kvPair.Value)
		batch.deleteBatch.AddDelete(key)
	}

	for _, fBatch := range batches {
		// Write to the remote shard
		log.Printf("Remote writing data from shard %d on node %d to remote shard %d", localShardID, p.cluster.GetNodeID(), fBatch.addBatch.ShardID)
		err := p.cluster.WriteBatch(fBatch.addBatch)
		if err != nil {
			return err
		}
		if del {
			// Delete locally
			log.Printf("Deleting keys from forwarder queue for shard %d on node %d", localShardID, p.cluster.GetNodeID())
			err = p.cluster.WriteBatch(fBatch.deleteBatch)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type forwardBatch struct {
	addBatch    *cluster.WriteBatch
	deleteBatch *cluster.WriteBatch
}

func (p *PushEngine) handleReceivedRows(receivingShardID uint64, rawRowHandler RawRowHandler) error {
	batch := cluster.NewWriteBatch(receivingShardID, false)
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID, receivingShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID+1, receivingShardID, 16)

	kvPairs, err := p.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return err
	}
	remoteConsumerRows := make(map[uint64][][]byte)
	receivingSequences := make(map[uint64]uint64)
	log.Printf("In handleReceivedRows on shard %d and node %d, Got %d rows in receiver table", receivingShardID, p.cluster.GetNodeID(), len(kvPairs))
	for _, kvPair := range kvPairs {
		sendingShardID := common.ReadUint64FromBufferBigEndian(kvPair.Key, 16)
		lastReceivedSeq, ok := receivingSequences[sendingShardID]
		if !ok {
			lastReceivedSeq, err = p.lastReceivingSequence(receivingShardID, sendingShardID)
			if err != nil {
				return err
			}
		}

		receivedSeq := common.ReadUint64FromBufferBigEndian(kvPair.Key, 24)
		remoteConsumerID := common.ReadUint64FromBufferBigEndian(kvPair.Key, 32)
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
		err = p.updateLastReceivingSequence(receivingShardID, sendingShardID, lastReceivedSequence, batch)
		if err != nil {
			return err
		}
	}
	return p.cluster.WriteBatch(batch)
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
		seqBytes, err := p.cluster.LocalGet(seqKey)
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

func (p *PushEngine) updateNextForwardSequence(localShardID uint64, sequence uint64, batch *cluster.WriteBatch) error {
	seqKey := p.genForwardSequenceKey(localShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	// TODO remove this lock!
	p.lock.RLock()
	defer p.lock.RUnlock()
	p.forwardSequences[localShardID] = sequence
	return nil
}

// TODO consider caching sequences in memory to avoid reading from storage each time
func (p *PushEngine) lastReceivingSequence(receivingShardID uint64, sendingShardID uint64) (uint64, error) {
	seqKey := p.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqBytes, err := p.cluster.LocalGet(seqKey)
	if err != nil {
		return 0, err
	}
	if seqBytes == nil {
		return 0, nil
	}
	return common.ReadUint64FromBufferLittleEndian(seqBytes, 0), nil
}

func (p *PushEngine) updateLastReceivingSequence(receivingShardID uint64, sendingShardID uint64, sequence uint64, batch *cluster.WriteBatch) error {
	seqKey := p.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	return nil
}

func (p *PushEngine) genForwardSequenceKey(localShardID uint64) []byte {
	return table.EncodeTableKeyPrefix(common.ForwarderSequenceTableID, localShardID, 16)
}

func (p *PushEngine) genReceivingSequenceKey(receivingShardID uint64, sendingShardID uint64) []byte {
	seqKey := table.EncodeTableKeyPrefix(common.ReceiverSequenceTableID, receivingShardID, 24)
	return common.AppendUint64ToBufferBigEndian(seqKey, sendingShardID)
}
