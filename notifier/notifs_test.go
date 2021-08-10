package notifier

import (
	"fmt"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// We test primarily with SessionClosedMessage as this allows us to pass simply an arbitrarily sized string so we can
// test notifications with various sizes

func TestSimpleNotificationOneServer(t *testing.T) {
	testSimpleNotification(t, 1)
}

func TestSimpleNotificationThreeServers(t *testing.T) {
	testSimpleNotification(t, 3)
}

func TestMultipleNotificationsDifferentSizes(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	var notifsToSend []string

	// Generate a bunch of notifications of various sizes
	numNotifs := 100
	for i := 0; i < numNotifs; i++ {
		notifSize := int(rnd.Int31n(10000) + 1)
		notif := make([]byte, notifSize)
		for j := 0; j < notifSize; j++ {
			notif[j] = byte(48 + j%30)
		}
		notifsToSend = append(notifsToSend, string(notif))
	}

	servers, listeners := testNotifications(t, 3, notifsToSend...)
	defer stopServers(t, servers...)

	for _, listener := range listeners {
		recNotifs := listener.Notifications()
		require.Equal(t, numNotifs, len(recNotifs))
		for i, exp := range notifsToSend {
			recNotif := recNotifs[i].(*notifications.SessionClosedMessage) //nolint: forcetypeassert
			require.Equal(t, exp, recNotif.SessionId)
		}
	}
}

func TestBigNotification(t *testing.T) {
	size := int(readBuffSize * 2.5)

	notif := make([]byte, size)
	for i := 0; i < size; i++ {
		notif[i] = byte(48 + i%10)
	}

	servers, listeners := testNotifications(t, 3, string(notif))
	defer stopServers(t, servers...)

	for _, listener := range listeners {
		recNotifs := listener.Notifications()
		require.Equal(t, 1, len(recNotifs))
		recNotif := recNotifs[0].(*notifications.SessionClosedMessage) //nolint: forcetypeassert
		require.Equal(t, string(notif), recNotif.SessionId)
	}
}

func TestNotificationStopServer(t *testing.T) {
	servers, listeners := startServers(t, 3)
	defer stopServers(t, servers...)

	var listenAddresses []string
	for _, server := range servers {
		listenAddresses = append(listenAddresses, server.ListenAddress())
	}
	client := newClient(listenAddresses...)
	err := client.Start()
	require.NoError(t, err)
	defer stopClient(t, client)

	sendAndReceiveNotif(t, client, "aardvarks", listeners)

	err = servers[1].Stop()
	require.NoError(t, err)

	var listenersWithout1 []*notifListener
	for i, listener := range listeners {
		if i != 1 {
			listenersWithout1 = append(listenersWithout1, listener)
		}
		listener.ClearNotifs()
	}
	sendAndReceiveNotif(t, client, "antelopes", listenersWithout1)
	listener1 := listeners[1]
	require.Equal(t, 0, len(listener1.Notifications()))
}

func TestNotificationsRetryConnections(t *testing.T) {
	servers, _ := startServers(t, 3)
	defer stopServers(t, servers...)

	var listenAddresses []string
	for _, server := range servers {
		listenAddresses = append(listenAddresses, server.ListenAddress())
	}
	client := newClient(listenAddresses...)
	err := client.Start()
	require.NoError(t, err)
	defer stopClient(t, client)

	numSent := 0
	err = client.BroadcastNotification(&notifications.SessionClosedMessage{SessionId: fmt.Sprintf("foo%d", numSent)})
	require.NoError(t, err)
	numSent++
	require.Equal(t, 3, client.numAvailableServers())
	require.Equal(t, 0, client.numUnavailableServers())

	err = servers[1].Stop()
	require.NoError(t, err)
	start := time.Now()
	for time.Now().Sub(start) < 5*time.Second {
		err := client.BroadcastNotification(&notifications.SessionClosedMessage{SessionId: fmt.Sprintf("foo%d", numSent)})
		require.NoError(t, err)
		numSent++

		if client.numUnavailableServers() == 1 {
			break
		}
	}
	// One server should become unavailable
	require.Equal(t, 1, client.numUnavailableServers())
	require.Equal(t, 2, client.numAvailableServers())

	// Now restart the server
	err = servers[1].Start()
	require.NoError(t, err)

	start = time.Now()
	for time.Now().Sub(start) < 5*time.Second {
		err := client.BroadcastNotification(&notifications.SessionClosedMessage{SessionId: fmt.Sprintf("foo%d", numSent)})
		require.NoError(t, err)
		numSent++

		if client.numUnavailableServers() == 0 {
			break
		}
	}
	// All servers should be available now
	require.Equal(t, 0, client.numUnavailableServers())
	require.Equal(t, 3, client.numAvailableServers())
}

func sendAndReceiveNotif(t *testing.T, client *client, notif string, listeners []*notifListener) {
	t.Helper()
	err := client.BroadcastNotification(&notifications.SessionClosedMessage{SessionId: notif})
	require.NoError(t, err)
	waitForNotifications(t, listeners, 1)
	for _, listener := range listeners {
		recNotifs := listener.Notifications()
		require.Equal(t, 1, len(recNotifs))
		recNotif := recNotifs[0].(*notifications.SessionClosedMessage) //nolint: forcetypeassert
		require.Equal(t, notif, recNotif.SessionId)
	}
}

func TestNotificationsMultipleConnections(t *testing.T) {

	servers, listeners := startServers(t, 3)
	defer stopServers(t, servers...)

	var listenAddresses []string
	for _, server := range servers {
		listenAddresses = append(listenAddresses, server.ListenAddress())
	}

	numClients := 10
	clients := make([]*client, 10)

	for i := 0; i < numClients; i++ {
		client := newClient(listenAddresses...)
		err := client.Start()
		require.NoError(t, err)
		clients[i] = client
	}
	defer stopClients(t, clients)

	var notifs []string
	numNotifications := 10
	for i := 0; i < numClients; i++ {
		client := clients[i]
		for j := 0; j < numNotifications; j++ {
			notif := fmt.Sprintf("notif%d", j)
			notifs = append(notifs, notif)
			err := client.BroadcastNotification(&notifications.SessionClosedMessage{SessionId: notif})
			require.NoError(t, err)
		}
	}

	totNotifs := numNotifications * numClients
	waitForNotifications(t, listeners, totNotifs)

	for _, listener := range listeners {
		recNotifs := listener.Notifications()
		require.Equal(t, totNotifs, len(recNotifs))
		for i, expNotif := range notifs {
			recNotif := recNotifs[i].(*notifications.SessionClosedMessage) //nolint: forcetypeassert
			require.Equal(t, expNotif, recNotif.SessionId)
		}
	}
}

func testSimpleNotification(t *testing.T, numServers int) {
	t.Helper()
	notif := "aardvarks!"

	servers, listeners := testNotifications(t, numServers, notif)
	defer stopServers(t, servers...)

	for _, listener := range listeners {
		recNotifs := listener.Notifications()
		require.Equal(t, 1, len(recNotifs))
		recNotif := recNotifs[0].(*notifications.SessionClosedMessage) //nolint: forcetypeassert
		require.Equal(t, notif, recNotif.SessionId)
	}
}

func testNotifications(t *testing.T, numServers int, notifsToSend ...string) ([]*server, []*notifListener) {
	t.Helper()

	servers, notifListeners := startServers(t, numServers)

	var listenAddresses []string

	for _, server := range servers {
		listenAddresses = append(listenAddresses, server.ListenAddress())
	}

	client := newClient(listenAddresses...)
	err := client.Start()
	require.NoError(t, err)
	defer stopClient(t, client)

	notifs := make([]Notification, len(notifsToSend))

	for i, str := range notifsToSend {
		notifs[i] = &notifications.SessionClosedMessage{
			SessionId: str,
		}
		err := client.BroadcastNotification(notifs[i])
		require.NoError(t, err)
	}

	waitForNotifications(t, notifListeners, len(notifsToSend))

	return servers, notifListeners
}

func TestMultipleNotificationTypes(t *testing.T) {
	t.Helper()

	notifListener1 := &notifListener{}
	notifListener2 := &notifListener{}

	server := newServer("localhost:7888")
	defer stopServers(t, server)
	server.RegisterNotificationListener(NotificationTypeDDLStatement, notifListener1)
	server.RegisterNotificationListener(NotificationTypeCloseSession, notifListener2)

	err := server.Start()
	require.NoError(t, err)

	client := newClient("localhost:7888")
	err = client.Start()
	require.NoError(t, err)
	defer stopClient(t, client)

	scMessage := &notifications.SessionClosedMessage{SessionId: "foo"}
	err = client.BroadcastNotification(scMessage)
	require.NoError(t, err)

	ddlMessage := &notifications.DDLStatementInfo{
		OriginatingNodeId: 1,
		SchemaName:        "whateva",
		Sql:               "some sql",
		TableSequences:    []uint64{1, 2, 3},
	}
	err = client.BroadcastNotification(ddlMessage)
	require.NoError(t, err)

	waitForNotifications(t, []*notifListener{notifListener1}, 1)

	ddlRec := notifListener1.notifs[0].(*notifications.DDLStatementInfo) //nolint: forcetypeassert
	require.Equal(t, ddlMessage.Sql, ddlRec.Sql)
	require.Equal(t, ddlMessage.OriginatingNodeId, ddlRec.OriginatingNodeId)
	require.Equal(t, ddlMessage.SchemaName, ddlRec.SchemaName)
	require.Equal(t, ddlMessage.TableSequences, ddlRec.TableSequences)

	waitForNotifications(t, []*notifListener{notifListener2}, 1)
	require.Equal(t, scMessage.SessionId, notifListener2.notifs[0].(*notifications.SessionClosedMessage).SessionId) //nolint: forcetypeassert
}

func waitForNotifications(t *testing.T, notifListeners []*notifListener, numNotificatiuons int) {
	t.Helper()
	commontest.WaitUntil(t, func() (bool, error) {
		for _, listener := range notifListeners {
			if numNotificatiuons != len(listener.Notifications()) {
				return false, nil
			}
		}
		return true, nil
	})
}

func stopServers(t *testing.T, servers ...*server) {
	t.Helper()
	for _, server := range servers {
		err := server.Stop()
		require.NoError(t, err)
	}
}

func stopClients(t *testing.T, clients []*client) {
	t.Helper()
	for _, client := range clients {
		stopClient(t, client)
	}
}

func stopClient(t *testing.T, client *client) {
	t.Helper()
	err := client.Stop()
	require.NoError(t, err)
}

func startServers(t *testing.T, numServers int) ([]*server, []*notifListener) {
	t.Helper()
	servers := make([]*server, numServers)
	notifListeners := make([]*notifListener, numServers)
	for i := 0; i < numServers; i++ {
		listenPort := 7888 + i
		listenAddress := fmt.Sprintf("localhost:%d", listenPort)
		server := newServer(listenAddress)
		notifListener := &notifListener{}
		server.RegisterNotificationListener(NotificationTypeCloseSession, notifListener)
		notifListeners[i] = notifListener
		err := server.Start()
		require.NoError(t, err)
		servers[i] = server
	}
	return servers, notifListeners
}

type notifListener struct {
	notifs []Notification
	lock   sync.Mutex
}

func (n *notifListener) HandleNotification(notification Notification) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.notifs = append(n.notifs, notification)
}

func (n *notifListener) Notifications() []Notification {
	n.lock.Lock()
	defer n.lock.Unlock()
	return n.notifs
}

func (n *notifListener) ClearNotifs() {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.notifs = nil
}
