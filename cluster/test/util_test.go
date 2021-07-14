package cluster

import (
	"github.com/squareup/pranadb/cluster"
	"sync"
)

type testNotificationListener struct {
	lock   sync.Mutex
	notifs []cluster.Notification
}

func (t *testNotificationListener) HandleNotification(notification cluster.Notification) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.notifs = append(t.notifs, notification)
}

func (t *testNotificationListener) getNotifs() []cluster.Notification {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.notifs
}

func (t *testNotificationListener) clearNotifs() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.notifs = make([]cluster.Notification, 0)
}
