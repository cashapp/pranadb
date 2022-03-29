package push

import (
	"fmt"
	"testing"

	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protolib"

	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/table"

	"github.com/squareup/pranadb/common/commontest"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/sharder"
)

func TestQueueForRemoteSend(t *testing.T) {
	clus, shard, pe := startup(t)
	testQueueForRemoteSend(t, 1, clus, shard, pe)
}

func TestQueueForRemoteSendWithPersistedSequence(t *testing.T) {
	clus, shard, pe := startup(t)
	// Update the sequence
	seqKey := table.EncodeTableKeyPrefix(common.ForwarderSequenceTableID, cluster.DataShardIDBase, 16)
	seqValueBytes := make([]byte, 0, 8)
	seqValueBytes = common.AppendUint64ToBufferLE(seqValueBytes, 333)
	batch := cluster.NewWriteBatch(cluster.DataShardIDBase, false)
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
	localShardID := cluster.DataShardIDBase

	rf := common.NewRowsFactory(colTypes)
	rows := queueRows(t, numRows, colTypes, rf, shard, pe, localShardID, clus, localShardID)

	keyStartPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID+1, localShardID, 16)

	kvPairs, err := clus.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	require.NoError(t, err)
	require.Equal(t, numRows, len(kvPairs))

	sched, _ := pe.GetScheduler(localShardID)

	err, ok := <-sched.ScheduleAction(func() error {
		// This needs to be called on the scheduler goroutine
		return pe.Mover().TransferData(localShardID, true)
	})
	require.True(t, ok)
	require.NoError(t, err)

	// Make sure data has been deleted from forwarder table
	kvPairs, err = clus.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	require.NoError(t, err)
	require.Equal(t, 0, len(kvPairs))

	// All the rows should be in the receiver table - this happens async so we must wait
	waitUntilRowsInReceiverTable(t, clus, numRows)

	// Check individual receiver rows
	for i, rowToSend := range rows {
		keyBytes := table.EncodeTableKeyPrefix(common.ReceiverTableID, rowToSend.remoteShardID, 40)
		keyBytes = common.AppendUint64ToBufferBE(keyBytes, localShardID)
		keyBytes = common.AppendUint64ToBufferBE(keyBytes, uint64(i+1))
		keyBytes = common.AppendUint64ToBufferBE(keyBytes, rowToSend.remoteConsumerID)
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
		sched, _ := pe.GetScheduler(sendingShardID)
		err, ok := <-sched.ScheduleAction(func() error {
			// This needs to be called on the scheduler goroutine
			return pe.Mover().TransferData(sendingShardID, true)
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
		hasForwards, err := pe.Mover().HandleReceivedRows(receivingShardID, rawRowHandler)
		require.NoError(t, err)
		require.False(t, hasForwards)

		actualRowsByRemoteConsumer := make(map[uint64][]rowInfo)
		rawRows := rawRowHandler.rawRows
		receivedRows := rf.NewRows(1)
		rowCount := 0
		for remoteConsumerID, rr := range rawRows {
			consumerRows := make([]rowInfo, len(rr))
			actualRowsByRemoteConsumer[remoteConsumerID] = consumerRows
			for i, rrr := range rr {
				lpvb, _ := common.ReadUint32FromBufferLE(rrr, 0)
				currRowBytes := rrr[8+lpvb:]
				err := common.DecodeRow(currRowBytes, colTypes, receivedRows)
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
		keyStartPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID, receivingShardID, 16)
		keyEndPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID+1, receivingShardID, 16)
		recPairs, err := clus.LocalScan(keyStartPrefix, keyEndPrefix, -1)
		require.NoError(t, err)
		require.Nil(t, recPairs)

		expectedSequences, ok := receivedSequences[receivingShardID]
		require.True(t, ok)

		// Check the receiving sequences have been updated ok
		for _, sendingShardID := range shardIds {
			seqKey := table.EncodeTableKeyPrefix(common.ReceiverSequenceTableID, receivingShardID, 24)
			seqKey = common.AppendUint64ToBufferBE(seqKey, sendingShardID)

			seqBytes, err := clus.LocalGet(seqKey)
			require.NoError(t, err)
			if seqBytes != nil {
				lastSeq, _ := common.ReadUint64FromBufferLE(seqBytes, 0)
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
	localShardID := cluster.DataShardIDBase
	rf := common.NewRowsFactory(colTypes)
	rows := queueRows(t, numRows, colTypes, rf, shard, pe, localShardID, clus, localShardID)
	remoteShardsIds := make(map[uint64]bool)
	for _, row := range rows {
		remoteShardsIds[row.remoteShardID] = true
	}

	sched, _ := pe.GetScheduler(localShardID)
	err, ok := <-sched.ScheduleAction(func() error {
		// We set delete so the transfer doesn't delete the rows from the forward table
		return pe.Mover().TransferData(localShardID, false)
	})
	require.True(t, ok)
	require.NoError(t, err)

	waitUntilRowsInReceiverTable(t, clus, numRows)

	rowsHandled := 0
	for remoteShardID := range remoteShardsIds {
		rawRowHandler := &rawRowHandler{}
		hasForwards, err := pe.Mover().HandleReceivedRows(remoteShardID, rawRowHandler)
		require.NoError(t, err)
		require.False(t, hasForwards)
		for _, rr := range rawRowHandler.rawRows {
			rowsHandled += len(rr)
		}
	}

	require.Equal(t, numRows, rowsHandled)

	// Make sure rows still in forwarder table
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 16)
	keyEndPrefixPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID+1, localShardID, 16)
	kvPairs, err := clus.LocalScan(keyStartPrefix, keyEndPrefixPrefix, -1)
	require.NoError(t, err)
	require.Equal(t, numRows, len(kvPairs))

	// Check forwarder sequence
	forSeqKey := table.EncodeTableKeyPrefix(common.ForwarderSequenceTableID, localShardID, 16)
	seqBytes, err := clus.LocalGet(forSeqKey)
	require.NoError(t, err)
	require.NotNil(t, seqBytes)
	lastSeq, _ := common.ReadUint64FromBufferLE(seqBytes, 0)
	require.Equal(t, uint64(numRows+1), lastSeq)

	// Check receiver sequence
	maxSeq := uint64(0)
	for remoteShardID := range remoteShardsIds {
		recSeqKey := table.EncodeTableKeyPrefix(common.ReceiverSequenceTableID, remoteShardID, 24)
		recSeqKey = common.AppendUint64ToBufferBE(recSeqKey, localShardID)

		seqBytes, err := clus.LocalGet(recSeqKey)
		require.NoError(t, err)
		if seqBytes != nil {
			lastSeq, _ := common.ReadUint64FromBufferLE(seqBytes, 0)
			if lastSeq > maxSeq {
				maxSeq = lastSeq
			}
		}
	}
	require.Equal(t, uint64(numRows), maxSeq)

	// Make sure rows deleted from receiver table
	exists, err := pe.ExistRowsInLocalTable(common.ReceiverTableID, clus.GetLocalShardIDs())
	require.NoError(t, err)
	require.False(t, exists)

	// Now try and forward them again
	err, ok = <-sched.ScheduleAction(func() error {
		return pe.Mover().TransferData(localShardID, true)
	})
	require.True(t, ok)
	require.NoError(t, err)

	// Wait for rows to be forwarded
	waitUntilRowsInReceiverTable(t, clus, numRows)

	// But they shouldn't be handled as they're seen before
	rowsHandled = 0
	for remoteShardID := range remoteShardsIds {
		rawRowHandler := &rawRowHandler{}
		hasForwards, err := pe.Mover().HandleReceivedRows(remoteShardID, rawRowHandler)
		require.NoError(t, err)
		require.False(t, hasForwards)
		for _, rr := range rawRowHandler.rawRows {
			rowsHandled += len(rr)
		}
	}

	require.Equal(t, 0, rowsHandled)
}

func testQueueForRemoteSend(t *testing.T, startSequence int, store cluster.Cluster, shard *sharder.Sharder, pe *Engine) {
	t.Helper()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType}
	localShardID := cluster.DataShardIDBase

	numRows := 10
	rf := common.NewRowsFactory(colTypes)

	rows := queueRows(t, numRows, colTypes, rf, shard, pe, localShardID, store, localShardID)

	keyStartPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ForwarderTableID+1, localShardID, 16)
	kvPairs, err := store.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	require.NoError(t, err)
	require.Equal(t, numRows, len(kvPairs))

	for i, rowToSend := range rows {

		keyBytes := table.EncodeTableKeyPrefix(common.ForwarderTableID, localShardID, 40)
		keyBytes = common.AppendUint64ToBufferBE(keyBytes, rowToSend.remoteShardID)
		keyBytes = common.AppendUint64ToBufferBE(keyBytes, uint64(i+startSequence))
		keyBytes = common.AppendUint64ToBufferBE(keyBytes, rowToSend.remoteConsumerID)

		loadRowAndVerifySame(t, keyBytes, rowToSend.row, store, colTypes, rf)
	}

	// Check forward sequence has been updated ok
	seqKey := table.EncodeTableKeyPrefix(common.ForwarderSequenceTableID, localShardID, 16)
	seqBytes, err := store.LocalGet(seqKey)
	require.NoError(t, err)
	require.NotNil(t, seqBytes)

	lastSeq, _ := common.ReadUint64FromBufferLE(seqBytes, 0)
	require.Equal(t, uint64(numRows+startSequence), lastSeq)
}

func startup(t *testing.T) (cluster.Cluster, *sharder.Sharder, *Engine) {
	t.Helper()
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	clus := cluster.NewFakeCluster(1, 10)
	metaController := meta.NewController(clus)
	shard := sharder.NewSharder(clus)
	config := conf.NewTestConfig(0)
	pe := NewPushEngine(clus, shard, metaController, config, &dummySimpleQueryExecutor{}, protolib.EmptyRegistry, nil)
	clus.RegisterShardListenerFactory(&delegatingShardListenerFactory{delegate: pe})
	clus.SetRemoteQueryExecutionCallback(&cluster.DummyRemoteQueryExecutionCallback{})
	err := clus.Start()
	require.NoError(t, err)
	err = shard.Start()
	require.NoError(t, err)
	err = pe.Start()
	require.NoError(t, err)
	return clus, shard, pe
}

type dummySimpleQueryExecutor struct {
}

func (d *dummySimpleQueryExecutor) ExecuteQuery(schemaName string, query string) (rows *common.Rows, err error) {
	return nil, nil
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

func (d delegatingShardListener) RemoteWriteOccurred(ingest bool) {
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
	lpvb, _ := common.ReadUint32FromBufferLE(v, 0)
	currRowBytes := v[8+lpvb:]
	err = common.DecodeRow(currRowBytes, colTypes, fRows)
	require.NoError(t, err)
	row := fRows.GetRow(0)
	commontest.RowsEqual(t, *expectedRow, row, colTypes)
}

// nolint: unparam
func queueRows(t *testing.T, numRows int, colTypes []common.ColumnType, rf *common.RowsFactory, shard *sharder.Sharder, pe *Engine,
	localShardID uint64, store cluster.Cluster, sendingShardID uint64) []rowInfo {
	t.Helper()
	rows := generateRows(t, numRows, colTypes, shard, rf, localShardID)
	batch := cluster.NewWriteBatch(sendingShardID, false)
	for _, rowToSend := range rows {
		_, err := pe.Mover().QueueRowForRemoteSend(rowToSend.remoteShardID, nil, rowToSend.row, sendingShardID, rowToSend.remoteConsumerID, colTypes, batch)
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
	shardIDs := stor.GetAllShardIDs()
	commontest.WaitUntil(t, func() (bool, error) {
		nr, err := numRowsInTable(stor, common.ReceiverTableID, shardIDs)
		if err != nil {
			return false, errors.WithStack(err)
		}
		return nr == numRows, nil
	})
}

func numRowsInTable(stor cluster.Cluster, tableID uint64, shardIDs []uint64) (int, error) {
	numRows := 0
	for _, shardID := range shardIDs {
		startPrefix := table.EncodeTableKeyPrefix(tableID, shardID, 16)
		endPrefix := table.EncodeTableKeyPrefix(tableID+1, shardID, 16)
		kvPairs, err := stor.LocalScan(startPrefix, endPrefix, -1)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		numRows += len(kvPairs)
	}
	return numRows, nil
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
