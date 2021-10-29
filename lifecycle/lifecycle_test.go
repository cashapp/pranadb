package lifecycle

import (
	"fmt"
	"github.com/squareup/pranadb/conf"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestStartedHandlerActive(t *testing.T) {
	testHandler(t, true, "/started")
}

func TestStartedHandlerInactive(t *testing.T) {
	testHandler(t, false, "/started")
}

func TestLivenessHandlerActive(t *testing.T) {
	testHandler(t, true, "/liveness")
}

func TestLivenessHandlerInactive(t *testing.T) {
	testHandler(t, false, "/liveness")
}

func TestReadinessHandlerActive(t *testing.T) {
	testHandler(t, true, "/readiness")
}

func TestReadinessHandlerInactive(t *testing.T) {
	testHandler(t, false, "/readiness")
}

func testHandler(t *testing.T, active bool, path string) {
	t.Helper()
	cnf := conf.NewDefaultConfig()
	cnf.EnableLifecycleEndpoint = true
	cnf.LifeCycleListenAddress = "localhost:8913"
	cnf.StartupEndpointPath = "/started"
	cnf.LiveEndpointPath = "/liveness"
	cnf.ReadyEndpointPath = "/readiness"

	hndlr := NewLifecycleEndpoints(*cnf)
	err := hndlr.Start()
	hndlr.SetActive(active)
	require.NoError(t, err)

	uri := fmt.Sprintf("http://%s%s", cnf.LifeCycleListenAddress, path)
	resp, err := http.Get(uri) //nolint:gosec
	require.NoError(t, err)
	if active {
		require.Equal(t, http.StatusOK, resp.StatusCode)
	} else {
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	}
	err = resp.Body.Close()
	require.NoError(t, err)

	err = hndlr.Stop()
	require.NoError(t, err)
}
