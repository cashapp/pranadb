package cluster

import (
	"github.com/squareup/pranadb/common"
)

type DummyShardListenerFactory struct {
}

func (d *DummyShardListenerFactory) CreateShardListener(shardID uint64) ShardListener {
	return &dummyShardListener{}
}

type dummyShardListener struct {
}

func (d *dummyShardListener) RemoteWriteOccurred(forwardRows []ForwardRow) {
}

func (d *dummyShardListener) Close() {
}

type DummyRemoteQueryExecutionCallback struct {
}

func (d *DummyRemoteQueryExecutionCallback) ExecuteRemotePullQuery(queryInfo *QueryExecutionInfo) (*common.Rows, error) {
	return nil, nil
}
