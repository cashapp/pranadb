package push

import (
	"fmt"
	"github.com/squareup/pranadb/common/commontest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/sharder"
)

// TODO write a test that hammers the mover with multiple receivers and senders different shards
// and verifies everything received ok and no duplicates
// TODO as above but inject failures at different points and test resend logic

func TestQueueForRemoteSend(t *testing.T) {
	clus, shard, pe := startup(t)
	testQueueForRemoteSend(t, 1, clus, shard, pe)
}

func TestQueueForRemoteSendWithPersistedSequence(t *testing.T) {
	clus, shard, pe := startup(t)
	// Update the sequence
	seqKey := make([]byte, 0, 16)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, common.ForwarderSequenceTableID)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, 1)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLittleEndian(seqValueBytes, 333)
	batch := cluster.NewWriteBatch(1, false)
	batch.AddPut(seqKey, seqValueBytes)
	err := clus.WriteBatch(batch)
	require.NoError(t, err)

	testQueueForRemoteSend(t, 333, clus, shard, pe)
}

func TestTransferData(t *testing.T) {
	clus, shard, pe := startup(t)

	// First get some rows in the forwarder table
	numRows := 10
	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType}
	localShardID := uint64(1)

	rf := common.NewRowsFactory(colTypes)
	rows := queueRows(t, numRows, colTypes, rf, shard, pe, localShardID, clus, localShardID)

	keyStartPrefix := createForwarderKey(localShardID)
	kvPairs, err := clus.LocalScan(keyStartPrefix, keyStartPrefix, -1)
	require.NoError(t, err)
	require.Equal(t, numRows, len(kvPairs))

	sched := pe.schedulers[localShardID]

	err, ok := <-sched.ScheduleAction(func() error {
		// This needs to be called on the scheduler goroutine
		return pe.transferData(localShardID, true)
	})
	require.True(t, ok)
	require.NoError(t, err)

	// Make sure data has been deleted from forwarder table
	kvPairs, err = clus.LocalScan(keyStartPrefix, keyStartPrefix, -1)
	require.NoError(t, err)
	require.Equal(t, 0, len(kvPairs))

	// All the rows should be in the receiver table - this happens async so we must wait
	waitUntilRowsInReceiverTable(t, clus, numRows)

	// TODO(tfox): Use this for something?
	// remoteKeyPrefix := make([]byte, 0)
	// remoteKeyPrefix = common.AppendUint64ToBufferLittleEndian(remoteKeyPrefix, ReceiverTableID)

	// Check individual receiver rows
	for i, rowToSend := range rows {
		keyBytes := make([]byte, 0, 40)
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, common.ReceiverTableID)
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, rowToSend.remoteShardID)
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, localShardID)
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, uint64(i+1))
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, rowToSend.remoteConsumerID)
		loadRowAndVerifySame(t, keyBytes, rowToSend.row, clus, colTypes, rf)
	}
}

func TestHandleReceivedRows(t *testing.T) {
	clus, shard, pe := startup(t)

	// First get some expectedRowsAtReceivingShard in the forwarder table
	numRows := 10
	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType}

	rf := common.NewRowsFactory(colTypes)

	shardIds := clus.GetAllShardIDs()

	var rows []rowInfo
	// We queue from each shard in the cluster
	for _, sendingShardID := range shardIds {

		latestRows := queueRows(t, numRows, colTypes, rf, shard, pe, sendingShardID, clus, sendingShardID)
		rows = append(rows, latestRows...)

		// Transfer to receiver table
		sched := pe.schedulers[sendingShardID]
		err, ok := <-sched.ScheduleAction(func() error {
			// This needs to be called on the scheduler goroutine
			return pe.transferData(sendingShardID, true)
		})
		require.True(t, ok)
		require.NoError(t, err)
	}

	waitUntilRowsInReceiverTable(t, clus, len(rows))

	rowsByReceivingShard := make(map[uint64][]rowInfo)
	for _, sent := range rows {
		rowsForReceiver := rowsByReceivingShard[sent.remoteShardID]
		rowsForReceiver = append(rowsForReceiver, sent)
		rowsByReceivingShard[sent.remoteShardID] = rowsForReceiver
	}

	// Compile a map of receiving_shard_id -> (map sending_shard_id -> last received sequence)
	receivedSequences := make(map[uint64]map[uint64]uint64)
	for _, rowToSend := range rows {
		seqsBySendingShardID, ok := receivedSequences[rowToSend.remoteShardID]
		if !ok {
			seqsBySendingShardID = make(map[uint64]uint64)
			receivedSequences[rowToSend.remoteShardID] = seqsBySendingShardID
		}
		seqsBySendingShardID[rowToSend.sendingShardID] = rowToSend.sendingSequence
	}

	for receivingShardID, expectedRowsAtReceivingShard := range rowsByReceivingShard {

		rawRowHandler := &rawRowHandler{}
		err := pe.handleReceivedRows(receivingShardID, rawRowHandler)
		require.NoError(t, err)

		actualRowsByRemoteConsumer := make(map[uint64][]rowInfo)
		rawRows := rawRowHandler.rawRows
		receivedRows := rf.NewRows(1)
		rowCount := 0
		for remoteConsumerID, rr := range rawRows {
			consumerRows := make([]rowInfo, len(rr))
			actualRowsByRemoteConsumer[remoteConsumerID] = consumerRows
			for i, rrr := range rr {
				err := common.DecodeRow(rrr, colTypes, receivedRows)
				require.NoError(t, err)
				actRow := receivedRows.GetRow(rowCount)
				receivedRowInfo := rowInfo{
					row:              &actRow,
					remoteConsumerID: remoteConsumerID,
					// sending shard id is not passed through
				}
				rowCount++
				consumerRows[i] = receivedRowInfo
			}
		}

		expectedRowsByRemoteConsumer := make(map[uint64][]rowInfo)
		for _, expectedRow := range expectedRowsAtReceivingShard {
			consumerRows, ok := expectedRowsByRemoteConsumer[expectedRow.remoteConsumerID]
			if !ok {
				consumerRows = make([]rowInfo, 0)
			}
			consumerRows = append(consumerRows, expectedRow)
			expectedRowsByRemoteConsumer[expectedRow.remoteConsumerID] = consumerRows
		}

		require.Equal(t, len(expectedRowsByRemoteConsumer), len(actualRowsByRemoteConsumer))

		for remoteConsumerID, expectedConsumerRows := range expectedRowsByRemoteConsumer {

			actualConsumerRows, ok := actualRowsByRemoteConsumer[remoteConsumerID]
			require.True(t, ok)

			require.Equal(t, len(expectedConsumerRows), len(actualConsumerRows))

			for i := 0; i < len(expectedConsumerRows); i++ {
				expectedRow := expectedConsumerRows[i]
				actualRow := actualConsumerRows[i]
				commontest.RowsEqual(t, *expectedRow.row, *actualRow.row, colTypes)
			}
		}

		// Make sure rows have been deleted from receiver table
		remoteKeyPrefix := make([]byte, 0)
		remoteKeyPrefix = common.AppendUint64ToBufferLittleEndian(remoteKeyPrefix, common.ReceiverTableID)
		remoteKeyPrefix = common.AppendUint64ToBufferLittleEndian(remoteKeyPrefix, receivingShardID)
		recPairs, err := clus.LocalScan(remoteKeyPrefix, remoteKeyPrefix, -1)
		require.NoError(t, err)
		require.Nil(t, recPairs)

		expectedSequences, ok := receivedSequences[receivingShardID]
		require.True(t, ok)

		// Check the receiving sequences have been updated ok
		for _, sendingShardID := range shardIds {
			seqKey := make([]byte, 0, 24)
			seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, common.ReceiverSequenceTableID)
			seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, receivingShardID)
			seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, sendingShardID)

			seqBytes, err := clus.LocalGet(seqKey)
			require.NoError(t, err)
			if seqBytes != nil {
				lastSeq := common.ReadUint64FromBufferLittleEndian(seqBytes, 0)
				expectedSeq, ok := expectedSequences[sendingShardID]
				require.True(t, ok)
				require.Equal(t, expectedSeq, lastSeq)
			} else {
				_, ok := expectedSequences[sendingShardID]
				require.False(t, ok)
			}
		}
	}
}

func TestDedupOfForwards(t *testing.T) {
	clus, shard, pe := startup(t)

	// Queue some rows, forward them
	numRows := 10
	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType}
	localShardID := uint64(1)
	rf := common.NewRowsFactory(colTypes)
	rows := queueRows(t, numRows, colTypes, rf, shard, pe, localShardID, clus, localShardID)
	remoteShardsIds := make(map[uint64]bool)
	for _, row := range rows {
		remoteShardsIds[row.remoteShardID] = true
	}

	sched := pe.schedulers[localShardID]
	err, ok := <-sched.ScheduleAction(func() error {
		// We set delete so the transfer doesn't delete the rows from the forward table
		return pe.transferData(localShardID, false)
	})
	require.True(t, ok)
	require.NoError(t, err)

	waitUntilRowsInReceiverTable(t, clus, numRows)

	rowsHandled := 0
	for remoteShardID := range remoteShardsIds {
		rawRowHandler := &rawRowHandler{}
		err := pe.handleReceivedRows(remoteShardID, rawRowHandler)
		require.NoError(t, err)
		for _, rr := range rawRowHandler.rawRows {
			rowsHandled += len(rr)
		}
	}

	require.Equal(t, numRows, rowsHandled)

	// Make sure rows still in forwarder table
	keyStartPrefix := createForwarderKey(localShardID)
	kvPairs, err := clus.LocalScan(keyStartPrefix, keyStartPrefix, -1)
	require.NoError(t, err)
	require.Equal(t, numRows, len(kvPairs))

	// Check forwarder sequence
	forSeqKey := make([]byte, 0, 16)
	forSeqKey = common.AppendUint64ToBufferLittleEndian(forSeqKey, common.ForwarderSequenceTableID)
	forSeqKey = common.AppendUint64ToBufferLittleEndian(forSeqKey, localShardID)
	seqBytes, err := pe.cluster.LocalGet(forSeqKey)
	require.NoError(t, err)
	require.NotNil(t, seqBytes)
	lastSeq := common.ReadUint64FromBufferLittleEndian(seqBytes, 0)
	require.Equal(t, uint64(numRows+1), lastSeq)

	// Check receiver sequence
	maxSeq := uint64(0)
	for remoteShardID := range remoteShardsIds {
		recSeqKey := make([]byte, 0, 24)
		recSeqKey = common.AppendUint64ToBufferLittleEndian(recSeqKey, common.ReceiverSequenceTableID)
		recSeqKey = common.AppendUint64ToBufferLittleEndian(recSeqKey, remoteShardID)
		recSeqKey = common.AppendUint64ToBufferLittleEndian(recSeqKey, localShardID)

		seqBytes, err := clus.LocalGet(recSeqKey)
		require.NoError(t, err)
		if seqBytes != nil {
			lastSeq := common.ReadUint64FromBufferLittleEndian(seqBytes, 0)
			if lastSeq > maxSeq {
				maxSeq = lastSeq
			}
		}
	}
	require.Equal(t, uint64(numRows), maxSeq)

	// Make sure rows deleted from receiver table
	remoteKeyPrefix := make([]byte, 0)
	remoteKeyPrefix = common.AppendUint64ToBufferLittleEndian(remoteKeyPrefix, common.ReceiverTableID)
	kvPairs, err = clus.LocalScan(remoteKeyPrefix, remoteKeyPrefix, -1)
	require.NoError(t, err)
	require.Nil(t, kvPairs)

	// Now try and forward them again
	err, ok = <-sched.ScheduleAction(func() error {
		return pe.transferData(localShardID, true)
	})
	require.True(t, ok)
	require.NoError(t, err)

	// Wait for rows to be forwarded
	waitUntilRowsInReceiverTable(t, clus, numRows)

	// But they shouldn't be handled as they're seen before
	rowsHandled = 0
	for remoteShardID := range remoteShardsIds {
		rawRowHandler := &rawRowHandler{}
		err := pe.handleReceivedRows(remoteShardID, rawRowHandler)
		require.NoError(t, err)
		for _, rr := range rawRowHandler.rawRows {
			rowsHandled += len(rr)
		}
	}

	require.Equal(t, 0, rowsHandled)
}

func testQueueForRemoteSend(t *testing.T, startSequence int, store cluster.Cluster, shard *sharder.Sharder, pe *PushEngine) {
	t.Helper()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType}
	localShardID := uint64(1)

	numRows := 10
	rf := common.NewRowsFactory(colTypes)

	rows := queueRows(t, numRows, colTypes, rf, shard, pe, localShardID, store, localShardID)

	keyStartPrefix := createForwarderKey(localShardID)
	kvPairs, err := store.LocalScan(keyStartPrefix, keyStartPrefix, -1)
	require.NoError(t, err)
	require.Equal(t, numRows, len(kvPairs))

	for i, rowToSend := range rows {

		var keyBytes []byte
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, common.ForwarderTableID)
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, localShardID)
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, rowToSend.remoteShardID)
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, uint64(i+startSequence))
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, rowToSend.remoteConsumerID)

		loadRowAndVerifySame(t, keyBytes, rowToSend.row, store, colTypes, rf)
	}

	// Check forward sequence has been updated ok
	seqKey := make([]byte, 0, 16)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, common.ForwarderSequenceTableID)
	seqKey = common.AppendUint64ToBufferLittleEndian(seqKey, localShardID)
	seqBytes, err := pe.cluster.LocalGet(seqKey)
	require.NoError(t, err)
	require.NotNil(t, seqBytes)

	lastSeq := common.ReadUint64FromBufferLittleEndian(seqBytes, 0)
	require.Equal(t, uint64(numRows+startSequence), lastSeq)
}

func startup(t *testing.T) (cluster.Cluster, *sharder.Sharder, *PushEngine) {
	t.Helper()
	clus := cluster.NewFakeCluster(1, 10)
	shard := sharder.NewSharder(clus)
	pe := NewPushEngine(clus, shard)
	clus.RegisterShardListenerFactory(&delegatingShardListenerFactory{delegate: pe})
	clus.SetRemoteQueryExecutionCallback(&dummyRemoteQueryExecutionCallback{})
	err := clus.Start()
	require.NoError(t, err)
	err = shard.Start()
	require.NoError(t, err)
	err = pe.Start()
	require.NoError(t, err)
	return clus, shard, pe
}

type delegatingShardListenerFactory struct {
	delegate cluster.ShardListenerFactory
}

func (d delegatingShardListenerFactory) CreateShardListener(shardID uint64) cluster.ShardListener {
	return &delegatingShardListener{delegate: d.delegate.CreateShardListener(shardID)}
}

type delegatingShardListener struct {
	delegate cluster.ShardListener
}

type dummyRemoteQueryExecutionCallback struct {
}

func (d dummyRemoteQueryExecutionCallback) ExecuteRemotePullQuery(pl *parplan.Planner, schemaName string, query string, queryID string, limit int, shardID uint64) (*common.Rows, error) {
	return nil, nil
}

func (d delegatingShardListener) RemoteWriteOccurred() {
	// Do nothing - we do not want to trigger remote writes in these tests
}

func (d delegatingShardListener) Close() {
	d.delegate.Close()
}

func loadRowAndVerifySame(t *testing.T, keyBytes []byte, expectedRow *common.Row, store cluster.Cluster, colTypes []common.ColumnType, rf *common.RowsFactory) {
	t.Helper()
	v, err := store.LocalGet(keyBytes)
	require.NoError(t, err)
	require.NotNil(t, v)
	fRows := rf.NewRows(1)
	require.NoError(t, err)
	err = common.DecodeRow(v, colTypes, fRows)
	require.NoError(t, err)
	row := fRows.GetRow(0)
	commontest.RowsEqual(t, *expectedRow, row, colTypes)
}

func createForwarderKey(localShardID uint64) []byte {
	keyStartPrefix := make([]byte, 0, 16)
	keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, common.ForwarderTableID)
	return common.AppendUint64ToBufferLittleEndian(keyStartPrefix, localShardID)
}

// nolint: unparam
func queueRows(t *testing.T, numRows int, colTypes []common.ColumnType, rf *common.RowsFactory, shard *sharder.Sharder, pe *PushEngine,
	localShardID uint64, store cluster.Cluster, sendingShardID uint64) []rowInfo {
	t.Helper()
	rows := generateRows(t, numRows, colTypes, shard, rf, localShardID)
	batch := cluster.NewWriteBatch(sendingShardID, false)
	for _, rowToSend := range rows {
		err := pe.QueueForRemoteSend(rowToSend.keyBuff, rowToSend.remoteShardID, rowToSend.row, sendingShardID, rowToSend.remoteConsumerID, colTypes, batch)
		require.NoError(t, err)
	}
	err := store.WriteBatch(batch)
	require.NoError(t, err)
	return rows
}

func generateRows(t *testing.T, numRows int, colTypes []common.ColumnType, sh *sharder.Sharder, rf *common.RowsFactory, sendingShardID uint64) []rowInfo {
	t.Helper()
	var rowsToSend []rowInfo
	rows := rf.NewRows(numRows)
	for i := 0; i < numRows; i++ {
		keyVal := int64(int(sendingShardID)*numRows + i)

		rows.AppendInt64ToColumn(0, keyVal)
		rows.AppendStringToColumn(1, fmt.Sprintf("some-string-%d", i))
		row := rows.GetRow(i)

		key := []interface{}{keyVal}
		var keyBuff []byte
		keyBuff, err := common.EncodeKey(key, colTypes, []int{0}, keyBuff)
		require.NoError(t, err)

		remoteShardID, err := sh.CalculateShard(sharder.ShardTypeHash, keyBuff)
		require.NoError(t, err)

		remoteConsumerID := uint64(i % 3)

		rowsToSend = append(rowsToSend, rowInfo{
			remoteConsumerID: remoteConsumerID,
			sendingSequence:  uint64(i + 1),
			sendingShardID:   sendingShardID,
			remoteShardID:    remoteShardID,
			keyBuff:          keyBuff,
			row:              &row,
		})
	}
	return rowsToSend
}

func waitUntilRowsInReceiverTable(t *testing.T, stor cluster.Cluster, numRows int) {
	t.Helper()
	remoteKeyPrefix := make([]byte, 0)
	remoteKeyPrefix = common.AppendUint64ToBufferLittleEndian(remoteKeyPrefix, common.ReceiverTableID)
	commontest.WaitUntil(t, func() (bool, error) {
		remPairs, err := stor.LocalScan(remoteKeyPrefix, remoteKeyPrefix, -1)
		if err != nil {
			return false, err
		}
		return numRows == len(remPairs), nil
	})
}

type rowInfo struct {
	remoteConsumerID uint64
	sendingSequence  uint64
	sendingShardID   uint64
	remoteShardID    uint64
	keyBuff          []byte
	row              *common.Row
}

type rawRowHandler struct {
	rawRows map[uint64][][]byte
}

func (r *rawRowHandler) HandleRawRows(rawRows map[uint64][][]byte, _ *cluster.WriteBatch) error {
	r.rawRows = rawRows
	return nil
}
