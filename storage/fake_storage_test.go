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

	writeBatch := createWriteBatchWithPuts(kvPair)

	partitionID := uint64(123545)

	err := storage.WriteBatch(partitionID, &writeBatch, false)
	require.Nil(t, err)

	res := storage.Get(partitionID, key)
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

	writeBatch := createWriteBatchWithPuts(kvPair)

	partitionID := uint64(123545)

	err := storage.WriteBatch(partitionID, &writeBatch, false)
	require.Nil(t, err)

	res := storage.Get(partitionID, key)
	require.NotNil(t, res)

	deleteBatch := createWriteBatchWithDeletes(key)

	err = storage.WriteBatch(partitionID, &deleteBatch, false)
	require.Nil(t, err)

	res = storage.Get(partitionID, key)
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
	wb := WriteBatch{puts: kvPairs}

	partitionID := uint64(123545)

	err := storage.WriteBatch(partitionID, &wb, false)
	require.Nil(t, err)

	keyStart := []byte("somekey456")
	keyEnd := []byte("somekey837")

	var res []KVPair
	res, err = storage.Scan(partitionID, keyStart, keyEnd, 1000)
	require.Nil(t, err)

	require.Equal(t, 837-456, len(res))
	for i, kvPair := range res {
		expectedK := fmt.Sprintf("somekey%03d", i+456)
		expectedV := fmt.Sprintf("somevalue%03d", i+456)
		require.Equal(t, expectedK, string(kvPair.Key))
		require.Equal(t, expectedV, string(kvPair.Value))
	}
}

func createWriteBatchWithPuts(puts ...KVPair) WriteBatch {
	return WriteBatch{puts: puts}
}

func createWriteBatchWithDeletes(deletes ...[]byte) WriteBatch {
	return WriteBatch{deletes: deletes}
}
