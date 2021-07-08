package cluster

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPutGet(t *testing.T) {

	clust := NewFakeCluster(0, 10)

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := clust.WriteBatch(&writeBatch)
	require.Nil(t, err)

	res, err := clust.LocalGet(key)
	require.Nil(t, err)
	require.NotNil(t, res)

	require.Equal(t, string(value), string(res))

}

func TestPutDelete(t *testing.T) {

	clust := NewFakeCluster(0, 10)

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := clust.WriteBatch(&writeBatch)
	require.Nil(t, err)

	res, err := clust.LocalGet(key)
	require.Nil(t, err)
	require.NotNil(t, res)

	deleteBatch := createWriteBatchWithDeletes(shardID, key)

	err = clust.WriteBatch(&deleteBatch)
	require.Nil(t, err)

	res, err = clust.LocalGet(key)
	require.Nil(t, err)
	require.Nil(t, res)
}

func TestScan(t *testing.T) {
	testScan(t, -1, 10)
}

func TestScanSmallLimit(t *testing.T) {
	testScan(t, 3, 3)
}

func TestScanBigLimit(t *testing.T) {
	testScan(t, 1000, 10)
}

func testScan(t *testing.T, limit int, expected int) {
	t.Helper()
	clust := NewFakeCluster(0, 10)

	var kvPairs []KVPair
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			k := []byte(fmt.Sprintf("foo-%02d/bar-%02d", i, j))
			v := []byte(fmt.Sprintf("somevalue%02d", j))
			kvPairs = append(kvPairs, KVPair{Key: k, Value: v})
		}
	}
	rand.Shuffle(len(kvPairs), func(i, j int) {
		kvPairs[i], kvPairs[j] = kvPairs[j], kvPairs[i]
	})
	shardID := uint64(123545)

	wb := NewWriteBatch(shardID, false)
	for _, kvPair := range kvPairs {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}

	err := clust.WriteBatch(wb)
	require.Nil(t, err)

	keyStart := []byte("foo-06")
	keyWhile := []byte("foo-06")

	var res []KVPair
	res, err = clust.LocalScan(keyStart, keyWhile, limit)
	require.Nil(t, err)

	require.Equal(t, expected, len(res))
	for i, kvPair := range res {
		expectedK := fmt.Sprintf("foo-06/bar-%02d", i)
		expectedV := fmt.Sprintf("somevalue%02d", i)
		require.Equal(t, expectedK, string(kvPair.Key))
		require.Equal(t, expectedV, string(kvPair.Value))
	}
}

func createWriteBatchWithPuts(shardID uint64, puts ...KVPair) WriteBatch {
	wb := NewWriteBatch(shardID, false)
	for _, kvPair := range puts {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}
	return *wb
}

func createWriteBatchWithDeletes(shardID uint64, deletes ...[]byte) WriteBatch {
	wb := NewWriteBatch(shardID, false)
	for _, delete := range deletes {
		wb.AddDelete(delete)
	}
	return *wb
}
