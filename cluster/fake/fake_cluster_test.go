/*
 *  Copyright 2022 Square Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package fake

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func startFakeCluster(t *testing.T) cluster.Cluster {
	t.Helper()
	clust := NewFakeCluster(0, 10)
	clust.RegisterShardListenerFactory(&cluster.DummyShardListenerFactory{})
	clust.SetRemoteQueryExecutionCallback(&cluster.DummyRemoteQueryExecutionCallback{})
	err := clust.Start()
	require.NoError(t, err)
	return clust
}

// nolint: unparam
func stopClustFunc(t *testing.T, clust cluster.Cluster) {
	t.Helper()
	err := clust.Stop()
	require.NoError(t, err)
}

func TestPutGet(t *testing.T) {

	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := cluster.KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := clust.WriteBatch(&writeBatch, false)
	require.NoError(t, err)

	res, err := clust.LocalGet(key)
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Equal(t, string(value), string(res))

}

func TestPutDelete(t *testing.T) {

	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := cluster.KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := clust.WriteBatch(&writeBatch, false)
	require.NoError(t, err)

	res, err := clust.LocalGet(key)
	require.NoError(t, err)
	require.NotNil(t, res)

	deleteBatch := createWriteBatchWithDeletes(shardID, key)

	err = clust.WriteBatch(&deleteBatch, false)
	require.NoError(t, err)

	res, err = clust.LocalGet(key)
	require.NoError(t, err)
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
	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	var kvPairs []cluster.KVPair
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			k := []byte(fmt.Sprintf("foo-%02d/bar-%02d", i, j))
			v := []byte(fmt.Sprintf("somevalue%02d", j))
			kvPairs = append(kvPairs, cluster.KVPair{Key: k, Value: v})
		}
	}
	rand.Shuffle(len(kvPairs), func(i, j int) {
		kvPairs[i], kvPairs[j] = kvPairs[j], kvPairs[i]
	})
	shardID := uint64(123545)

	wb := cluster.NewWriteBatch(shardID)
	for _, kvPair := range kvPairs {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}

	err := clust.WriteBatch(wb, false)
	require.NoError(t, err)

	keyStart := []byte("foo-06")
	keyEnd := []byte("foo-07")

	var res []cluster.KVPair
	res, err = clust.LocalScan(keyStart, keyEnd, limit)
	require.NoError(t, err)

	require.Equal(t, expected, len(res))
	for i, kvPair := range res {
		expectedK := fmt.Sprintf("foo-06/bar-%02d", i)
		expectedV := fmt.Sprintf("somevalue%02d", i)
		require.Equal(t, expectedK, string(kvPair.Key))
		require.Equal(t, expectedV, string(kvPair.Value))
	}
}

func createWriteBatchWithPuts(shardID uint64, puts ...cluster.KVPair) cluster.WriteBatch {
	wb := cluster.NewWriteBatch(shardID)
	for _, kvPair := range puts {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}
	return *wb
}

func createWriteBatchWithDeletes(shardID uint64, deletes ...[]byte) cluster.WriteBatch {
	wb := cluster.NewWriteBatch(shardID)
	for _, delete := range deletes {
		wb.AddDelete(delete)
	}
	return *wb
}

func TestRestart(t *testing.T) {

	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := cluster.KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := clust.WriteBatch(&writeBatch, false)
	require.NoError(t, err)

	// Now stop the cluster
	stopClustFunc(t, clust)

	// Restart it
	err = clust.Start()
	require.NoError(t, err)

	// Data should be there

	res, err := clust.LocalGet(key)
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Equal(t, string(value), string(res))
}

func TestGetReleaseLock(t *testing.T) {
	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	ok, err := clust.GetLock("/schema1")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.GetLock("/schema1")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = clust.GetLock("/schema2")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.GetLock("/schema2")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = clust.GetLock("/")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = clust.GetLock("/")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = clust.ReleaseLock("/schema1")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.ReleaseLock("/schema2")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.GetLock("/")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.GetLock("/schema1")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = clust.ReleaseLock("/")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.GetLock("/schema1")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.ReleaseLock("/schema1")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestLocksRestart(t *testing.T) {
	clust := startFakeCluster(t)
	stopClustFunc(t, clust)

	ok, err := clust.GetLock("/schema1")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.GetLock("/schema2")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.GetLock("/schema3")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.ReleaseLock("/schema2")
	require.NoError(t, err)
	require.True(t, ok)

	err = clust.Stop()
	require.NoError(t, err)
	err = clust.Start()
	require.NoError(t, err)
	defer stopClustFunc(t, clust)

	ok, err = clust.GetLock("/schema1")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = clust.GetLock("/schema2")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = clust.GetLock("/schema3")
	require.NoError(t, err)
	require.False(t, ok)
}
