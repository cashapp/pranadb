package cluster

import (
	"github.com/squareup/pranadb/common"
	"sync"
)

type TestNotificationListener struct {
	lock   sync.Mutex
	notifs []Notification
}

func (t *TestNotificationListener) HandleNotification(notification Notification) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.notifs = append(t.notifs, notification)
}

func (t *TestNotificationListener) getNotifs() []Notification {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.notifs
}

type DummyShardListenerFactory struct {
}

func (d *DummyShardListenerFactory) CreateShardListener(shardID uint64) ShardListener {
	return &dummyShardListener{}
}

type dummyShardListener struct {
}

func (d *dummyShardListener) RemoteWriteOccurred() {
}

func (d *dummyShardListener) Close() {
}

type DummyRemoteQueryExecutionCallback struct {
}

func (d *DummyRemoteQueryExecutionCallback) ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, shardID uint64) (*common.Rows, error) {
	return nil, nil
}
