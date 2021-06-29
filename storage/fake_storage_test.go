package storage

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestPutGet(t *testing.T) {

	storage := NewFakeStorage()

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := storage.WriteBatch(&writeBatch, false)
	require.Nil(t, err)

	res, err := storage.Get(shardID, key, true)
	require.Nil(t, err)
	require.NotNil(t, res)

	require.Equal(t, string(value), string(res))

}

func TestPutDelete(t *testing.T) {

	storage := NewFakeStorage()

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := storage.WriteBatch(&writeBatch, false)
	require.Nil(t, err)

	res, err := storage.Get(shardID, key, true)
	require.Nil(t, err)
	require.NotNil(t, res)

	deleteBatch := createWriteBatchWithDeletes(shardID, key)

	err = storage.WriteBatch(&deleteBatch, false)
	require.Nil(t, err)

	res, err = storage.Get(shardID, key, true)
	require.Nil(t, err)
	require.Nil(t, res)
}

// TODO test Scan with limit, Scan with no end range

func TestScan(t *testing.T) {

	storage := NewFakeStorage()

	var kvPairs []KVPair
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("somekey%03d", i))
		v := []byte(fmt.Sprintf("somevalue%03d", i))
		kvPairs = append(kvPairs, KVPair{Key: k, Value: v})
	}
	rand.Shuffle(len(kvPairs), func(i, j int) {
		kvPairs[i], kvPairs[j] = kvPairs[j], kvPairs[i]
	})
	shardID := uint64(123545)

	wb := NewWriteBatch(shardID)
	for _, kvPair := range kvPairs {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}

	err := storage.WriteBatch(wb, false)
	require.Nil(t, err)

	keyStart := []byte("somekey456")
	keyEnd := []byte("somekey837")

	var res []KVPair
	res, err = storage.Scan(keyStart, keyEnd, 1000)
	require.Nil(t, err)

	require.Equal(t, 837-456, len(res))
	for i, kvPair := range res {
		expectedK := fmt.Sprintf("somekey%03d", i+456)
		expectedV := fmt.Sprintf("somevalue%03d", i+456)
		require.Equal(t, expectedK, string(kvPair.Key))
		require.Equal(t, expectedV, string(kvPair.Value))
	}
}

func createWriteBatchWithPuts(shardID uint64, puts ...KVPair) WriteBatch {
	wb := NewWriteBatch(shardID)
	for _, kvPair := range puts {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}
	return *wb
}

func createWriteBatchWithDeletes(shardID uint64, deletes ...[]byte) WriteBatch {
	wb := NewWriteBatch(shardID)
	for _, delete := range deletes {
		wb.AddDelete(delete)
	}
	return *wb
}
