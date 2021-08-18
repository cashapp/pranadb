package wire

import (
	"context"
	"net"
	"testing"

	"github.com/squareup/pranadb/conf"
	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"github.com/squareup/pranadb/server"
)

func TestServer(t *testing.T) {
	logger := zaptest.NewLogger(t)
	psrv, err := server.NewServer(conf.Config{
		NodeID:     0,
		NumShards:  10,
		TestServer: true,
		Logger:     logger,
	})
	require.NoError(t, err)
	srv := grpc.NewServer(RegisterSessionManager(logger))
	defer srv.Stop()
	service.RegisterPranaDBServiceServer(srv, New(psrv, logger))
	l := bufconn.Listen(1024 * 1024)
	defer l.Close()
	go func() {
		err := srv.Serve(l)
		require.NoError(t, err)
	}()

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return l.Dial()
	}), grpc.WithInsecure())
	require.NoError(t, err)

	client := service.NewPranaDBServiceClient(conn)
	_, err = client.Use(context.Background(), &service.UseRequest{Schema: "test"})
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}
