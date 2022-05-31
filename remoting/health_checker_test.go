package remoting

import (
	"fmt"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestStartStopServers(t *testing.T) {
	servers, serverAddresses, al, ht := setup()

	// Start servers one by one
	waitUntilDesiredState(t, []bool{true, false, false}, serverAddresses, servers, al)
	waitUntilDesiredState(t, []bool{true, true, false}, serverAddresses, servers, al)
	waitUntilDesiredState(t, []bool{true, true, true}, serverAddresses, servers, al)

	// Stop one by one
	waitUntilDesiredState(t, []bool{false, true, true}, serverAddresses, servers, al)
	waitUntilDesiredState(t, []bool{false, false, true}, serverAddresses, servers, al)
	waitUntilDesiredState(t, []bool{false, false, false}, serverAddresses, servers, al)

	// And start all again
	waitUntilDesiredState(t, []bool{true, true, true}, serverAddresses, servers, al)

	ht.Stop()
	stopServers(t, servers...)
}

func TestNoHeartbeats(t *testing.T) {
	servers, serverAddresses, al, ht := setup()

	waitUntilDesiredState(t, []bool{true, true, true}, serverAddresses, servers, al)

	servers[0].DisableResponses()
	waitUntilDesiredState(t, []bool{false, true, true}, serverAddresses, servers, al)
	servers[1].DisableResponses()
	waitUntilDesiredState(t, []bool{false, false, true}, serverAddresses, servers, al)
	servers[2].DisableResponses()
	waitUntilDesiredState(t, []bool{false, false, false}, serverAddresses, servers, al)

	ht.Stop()
	stopServers(t, servers...)
}

func setup() (servers []*server, serverAddresses []string, al *availabilityListener, ht *HealthChecker) {
	servers = createServers(3)
	serverAddresses = make([]string, 3)
	for i := 0; i < 3; i++ {
		serverAddresses[i] = fmt.Sprintf("localhost:%d", 7888+i)
	}
	hbTimeout := 1 * time.Second
	hbInterval := 2 * time.Second
	ht = NewHealthChecker(serverAddresses, hbTimeout, hbInterval)
	al = newAvailabilityListener()
	ht.AddAvailabilityListener(al)
	ht.Start()
	return
}

// Start or stop the servers as appropriate until they are started/stopped according to desiredState - then verify
// availability status is correct
func waitUntilDesiredState(t *testing.T, desiredState []bool, serverAddresses []string, servers []*server, al *availabilityListener) {
	t.Helper()
	for i, desired := range desiredState {
		serverAddress := serverAddresses[i]
		current := al.getAvailability(serverAddress)
		if !current && desired {
			err := servers[i].Start()
			require.NoError(t, err)
		} else if current && !desired {
			err := servers[i].Stop()
			require.NoError(t, err)
		}
	}
	commontest.WaitUntil(t, func() (bool, error) {
		for i, desired := range desiredState {
			serverAddress := serverAddresses[i]
			current := al.getAvailability(serverAddress)
			if current != desired {
				return false, nil
			}
		}
		return true, nil
	})

}

type availabilityListener struct {
	availStatuses map[string]bool
	lock          sync.Mutex
}

func newAvailabilityListener() *availabilityListener {
	return &availabilityListener{
		availStatuses: map[string]bool{},
	}
}

func (a *availabilityListener) AvailabilityChanged(serverAddress string, available bool) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.availStatuses[serverAddress] = available
}

func (a *availabilityListener) getAvailability(serverAddress string) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.availStatuses[serverAddress]
}

func createServers(numServers int) []*server {
	servers := make([]*server, numServers)
	for i := 0; i < numServers; i++ {
		listenPort := 7888 + i
		listenAddress := fmt.Sprintf("localhost:%d", listenPort)
		servers[i] = newServer(listenAddress)
	}
	return servers
}
