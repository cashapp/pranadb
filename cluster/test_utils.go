package cluster

import (
	"sync"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/notifier"
)

type TestNotificationListener struct {
	lock   sync.Mutex
	notifs []notifier.Notification
}

func (t *TestNotificationListener) HandleNotification(notification notifier.Notification) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.notifs = append(t.notifs, notification)
	return nil
}

func (t *TestNotificationListener) getNotifs() []notifier.Notification {
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

func (d *DummyRemoteQueryExecutionCallback) ExecuteRemotePullQuery(queryInfo *QueryExecutionInfo) (*common.Rows, error) {
	return nil, nil
}
