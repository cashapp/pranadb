package dragon

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
)

func TestSaveRestoreSnapshot(t *testing.T) {
	srcDB, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	dstDB, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)

	shardIDs := []uint64{'\x2a', '\x2b', '\x2c'}
	tableID := uint64(100)
	shardIDToSnapshot := shardIDs[1]
	value := make([]byte, 10)

	// Fills each shard's table with n keys and random values
	randFill := func(db *pebble.DB, n int) {
		for _, shardID := range shardIDs {
			key := table.EncodeTableKeyPrefix(tableID, shardID, 20)
			key = append(key, []byte{0, 0, 0, 0}...)
			for i := 0; i < n; i++ {
				key = common.IncrementBytesBigEndian(key)
				rand.Read(value)
				require.NoError(t, db.Set(key, value, &pebble.WriteOptions{}))
			}
		}
	}

	randFill(srcDB, 100)
	randFill(dstDB, 200)

	buf := &bytes.Buffer{}
	snapshotPrefix := table.EncodeTableKeyPrefix(tableID, shardIDs[1], 20)
	require.NoError(t, saveSnapshotDataToWriter(srcDB.NewSnapshot(), snapshotPrefix, buf, shardIDToSnapshot))

	startPrefix := common.AppendUint64ToBufferBE(make([]byte, 0, 8), shardIDToSnapshot)
	endPrefix := common.AppendUint64ToBufferBE(make([]byte, 0, 8), shardIDToSnapshot+1)
	require.NoError(t, restoreSnapshotDataFromReader(dstDB, startPrefix, endPrefix, buf, os.TempDir()))

	startPrefix = table.EncodeTableKeyPrefix(tableID, shardIDToSnapshot, 20)
	endPrefix = common.AppendUint64ToBufferBE(make([]byte, 0, 8), shardIDToSnapshot+1)
	srcIter := srcDB.NewIter(&pebble.IterOptions{
		LowerBound: startPrefix,
		UpperBound: endPrefix,
	})
	dstIter := dstDB.NewIter(&pebble.IterOptions{
		LowerBound: startPrefix,
		UpperBound: endPrefix,
	})

	first := true
	count := 0
	for srcIter.First(); srcIter.Valid(); srcIter.Next() {
		if first {
			dstIter.First()
			first = false
		} else {
			dstIter.Next()
		}
		require.True(t, dstIter.Valid())

		require.Equal(t, srcIter.Key(), dstIter.Key())
		require.Equal(t, srcIter.Value(), dstIter.Value())

		count++
	}
	require.Equal(t, 100, count)
	require.False(t, dstIter.Next())
}
