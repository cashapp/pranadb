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

func (f *FakeNotifier) BroadcastNotification(notif Notification) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	listener, ok := f.notifListeners[TypeForNotification(notif)]
	if !ok {
		panic("no notification listener")
	}
	listener.HandleNotification(notif)
	return nil
}
