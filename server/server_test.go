package server

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestServerStart(t *testing.T) {

	server := *NewServer()
	require.Nil(t, server.Start())

}
