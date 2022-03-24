package mover

import (
	"sync"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/table"
)

type RawRowHandler interface {
	HandleRawRows(rawRows map[uint64][][]byte, batch *cluster.WriteBatch) error
}

type Mover struct {
	cluster          cluster.Cluster
	forwardSequences sync.Map
}

func NewMover(cluster cluster.Cluster) *Mover {
	return &Mover{cluster: cluster}
}

func (m *Mover) EncodeReceiverForIngestKey(receivingShardID uint64, dedupKey []byte, remoteConsumerID uint64) []byte {
	buff := table.EncodeTableKeyPrefix(common.ReceiverIngestTableID, receivingShardID, 56)
	if len(dedupKey) != 24 {
		panic("dedupTuple must have length 24")
	}
	buff = append(buff, dedupKey...)
	buff = common.AppendUint64ToBufferBE(buff, remoteConsumerID)
	return buff
}

func (m *Mover) QueueRowForRemoteSend(remoteShardID uint64, prevRow *common.Row, currRow *common.Row, localShardID uint64, remoteConsumerID uint64, colTypes []common.ColumnType, batch *cluster.WriteBatch) (int, error) {
	var prevValueBuff []byte
	if prevRow != nil {
		prevValueBuff = make([]byte, 0, 32)
		var err error
		prevValueBuff, err = common.EncodeRow(prevRow, colTypes, prevValueBuff)
		if err != nil {
			return 0, errors.WithStack(err)
		}
	}
	var currValueBuff []byte
	if currRow != nil {
		currValueBuff = make([]byte, 0, 32)
		var err error
		currValueBuff, err = common.EncodeRow(currRow, colTypes, currValueBuff)
		if err != nil {
			return 0, errors.WithStack(err)
		}
	}
	l := len(prevValueBuff) + len(currValueBuff)
	return l, m.QueueForRemoteSend(remoteShardID, prevValueBuff, currValueBuff, localShardID, remoteConsumerID, batch)
}

func (m *Mover) QueueForRemoteSend(remoteShardID uint64, prevValueBuff []byte, currValueBuff []byte, localShardID uint64, remoteConsumerID uint64, batch *cluster.WriteBatch) error {
	sequence, err := m.nextForwardSequence(localShardID)
	if err != nil {
		return errors.WithStack(err)
	}
	queueKeyBytes := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 40)
	queueKeyBytes = common.AppendUint64ToBufferBE(queueKeyBytes, remoteShardID)
	queueKeyBytes = common.AppendUint64ToBufferBE(queueKeyBytes, sequence)
	queueKeyBytes = common.AppendUint64ToBufferBE(queueKeyBytes, remoteConsumerID)
	buff := m.EncodePrevAndCurrentRow(prevValueBuff, currValueBuff)
	batch.AddPut(queueKeyBytes, buff)
	sequence++
	batch.HasForwards = true
	return m.updateNextForwardSequence(localShardID, sequence, batch)
}

func (m *Mover) EncodePrevAndCurrentRow(prevValueBuff []byte, currValueBuff []byte) []byte {
	lpvb := len(prevValueBuff)
	lcvb := len(currValueBuff)
	buff := make([]byte, 0, lpvb+lcvb+8)
	buff = common.AppendUint32ToBufferLE(buff, uint32(lpvb))
	buff = append(buff, prevValueBuff...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(lcvb))
	buff = append(buff, currValueBuff...)
	return buff
}

// TransferData TODO instead of reading from storage, we can pass rows from QueueRowForRemoteSend to here via
// a channel - this will avoid scan of storage
func (m *Mover) TransferData(localShardID uint64, del bool) error {
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID+1, localShardID, 16)

	kvPairs, err := m.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return errors.WithStack(err)
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
			return errors.WithStack(err)
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

func (m *Mover) HandleReceivedRows(receivingShardID uint64, rawRowHandler RawRowHandler) (bool, error) {
	batch := cluster.NewWriteBatch(receivingShardID, false)
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID, receivingShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID+1, receivingShardID, 16)

	kvPairs, err := m.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return false, errors.WithStack(err)
	}
	remoteConsumerRows := make(map[uint64][][]byte)
	receivingSequences := make(map[uint64]uint64)

	for _, kvPair := range kvPairs {
		sendingShardID, _ := common.ReadUint64FromBufferBE(kvPair.Key, 16)
		lastReceivedSeq, ok := receivingSequences[sendingShardID]
		if !ok {
			lastReceivedSeq, err = m.lastReceivingSequence(receivingShardID, sendingShardID)
			if err != nil {
				return false, errors.WithStack(err)
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
			return false, errors.WithStack(err)
		}
	}
	for sendingShardID, lastReceivedSequence := range receivingSequences {
		m.updateLastReceivingSequence(receivingShardID, sendingShardID, lastReceivedSequence, batch)
	}
	err = m.cluster.WriteBatch(batch)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return batch.HasForwards, nil
}

// HandleReceivedRowsForIngest - is used to receive rows that have been forwarded after ingest
func (m *Mover) HandleReceivedRowsForIngest(receivingShardID uint64, rawRowHandler RawRowHandler) (bool, error) {
	batch := cluster.NewWriteBatch(receivingShardID, false)
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ReceiverIngestTableID, receivingShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ReceiverIngestTableID+1, receivingShardID, 16)

	kvPairs, err := m.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return false, errors.WithStack(err)
	}
	remoteConsumerRows := make(map[uint64][][]byte)

	// Format of key is:
	// shard_id|receiver_ingest_table_id|originator_id|sequence|remote_consumer_id
	for _, kvPair := range kvPairs {
		remoteConsumerID, _ := common.ReadUint64FromBufferBE(kvPair.Key, 40)
		rows, ok := remoteConsumerRows[remoteConsumerID]
		if !ok {
			rows = make([][]byte, 0)
		}
		rows = append(rows, kvPair.Value)
		remoteConsumerRows[remoteConsumerID] = rows
		batch.AddDelete(kvPair.Key)
	}
	if len(remoteConsumerRows) > 0 {
		err = rawRowHandler.HandleRawRows(remoteConsumerRows, batch)
		if err != nil {
			return false, errors.WithStack(err)
		}
	}
	err = m.cluster.WriteBatch(batch)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return batch.HasForwards, nil
}

// TODO consider caching sequences in memory to avoid reading from storage each time
// Return the next forward sequence value
func (m *Mover) nextForwardSequence(localShardID uint64) (uint64, error) {
	var nextSeq uint64
	v, ok := m.forwardSequences.Load(localShardID)
	if !ok {
		seqKey := m.genForwardSequenceKey(localShardID)
		seqBytes, err := m.cluster.LocalGet(seqKey)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if seqBytes == nil {
			return 1, nil
		}
		nextSeq, _ = common.ReadUint64FromBufferLE(seqBytes, 0)
	} else {
		nextSeq = v.(uint64) //nolint:forcetypeassert
	}
	return nextSeq, nil
}

func (m *Mover) updateNextForwardSequence(localShardID uint64, sequence uint64, batch *cluster.WriteBatch) error {
	seqKey := m.genForwardSequenceKey(localShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLE(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
	m.forwardSequences.Store(localShardID, sequence)
	return nil
}

// TODO consider caching sequences in memory to avoid reading from storage each time
func (m *Mover) lastReceivingSequence(receivingShardID uint64, sendingShardID uint64) (uint64, error) {
	seqKey := m.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqBytes, err := m.cluster.LocalGet(seqKey)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if seqBytes == nil {
		return 0, nil
	}
	res, _ := common.ReadUint64FromBufferLE(seqBytes, 0)
	return res, nil
}

func (m *Mover) updateLastReceivingSequence(receivingShardID uint64, sendingShardID uint64, sequence uint64, batch *cluster.WriteBatch) {
	seqKey := m.genReceivingSequenceKey(receivingShardID, sendingShardID)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLE(seqValueBytes, sequence)
	batch.AddPut(seqKey, seqValueBytes)
}

func (m *Mover) genForwardSequenceKey(localShardID uint64) []byte {
	return table.EncodeTableKeyPrefix(common.ForwarderSequenceTableID, localShardID, 16)
}

func (m *Mover) genReceivingSequenceKey(receivingShardID uint64, sendingShardID uint64) []byte {
	seqKey := table.EncodeTableKeyPrefix(common.ReceiverSequenceTableID, receivingShardID, 24)
	return common.AppendUint64ToBufferBE(seqKey, sendingShardID)
}
