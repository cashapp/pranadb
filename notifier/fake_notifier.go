package notifier

import (
	"sync"
)

func NewFakeNotifier() *FakeNotifier {
	return &FakeNotifier{
		notifListeners: make(map[NotificationType]NotificationListener),
	}
}

type FakeNotifier struct {
	lock           sync.Mutex
	notifListeners map[NotificationType]NotificationListener
}

func (f *FakeNotifier) Start() error {
	return nil
}

func (f *FakeNotifier) Stop() error {
	return nil
}

func (f *FakeNotifier) RegisterNotificationListener(notificationType NotificationType, listener NotificationListener) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.notifListeners[notificationType] = listener
}

func (f *FakeNotifier) BroadcastOneway(notif Notification) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	listener, ok := f.notifListeners[TypeForNotification(notif)]
	if !ok {
		panic("no notification listener")
	}
	return listener.HandleNotification(notif)
}

func (f *FakeNotifier) BroadcastSync(notif Notification) error {
	return f.BroadcastOneway(notif)
}

func (f *FakeNotifier) ConnectionCount() int {
	return 0
}
