package wire

import (
	"context"
	"github.com/squareup/pranadb/conf"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"github.com/squareup/pranadb/server"
)

func TestServer(t *testing.T) {
	psrv, err := server.NewServer(conf.Config{
		NodeID:     0,
		NumShards:  10,
		TestServer: true,
	})
	require.NoError(t, err)
	srv := grpc.NewServer(RegisterSessionManager())
	defer srv.Stop()
	service.RegisterPranaDBServiceServer(srv, New(psrv))
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
