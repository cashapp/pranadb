package schema

import (
	"testing"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
	"github.com/stretchr/testify/require"
)

func TestLoader(t *testing.T) {
	clus := cluster.NewFakeCluster(1, 1)
	metaController, executor := runServer(t, clus)
	session := executor.CreateSession("map")
	_, err := executor.ExecuteSQLStatement(session, `create source location(
				id bigint,
				x varchar,
				y varchar,
				primary key (id)
			)`)
	require.NoError(t, err)
	session = executor.CreateSession("movies")
	_, err = executor.ExecuteSQLStatement(session, `create source actor(
				id bigint,
				name varchar,
				age int,
				primary key (id)
			)`)
	require.NoError(t, err)
	_, err = executor.ExecuteSQLStatement(session, `create source movies(
				id bigint,
				title varchar,
				director varchar,
				year int,
				primary key (id)
			)`)
	require.NoError(t, err)

	mapSchema, ok := metaController.GetSchema("map")
	require.True(t, ok)
	require.Len(t, mapSchema.Tables, 1)
	moviesSchema, ok := metaController.GetSchema("movies")
	require.True(t, ok)
	require.Len(t, moviesSchema.Tables, 2)

	// Restart the server
	_ = clus.Stop()
	metaController, executor = runServer(t, clus)
	_, ok = metaController.GetSchema("test")
	require.False(t, ok)

	loader := NewLoader(metaController, executor, executor.GetPushEngine())
	require.NoError(t, loader.Start())

	actualSchema, ok := metaController.GetSchema("map")
	require.True(t, ok)
	require.Equal(t, mapSchema, actualSchema)
	actualSchema, ok = metaController.GetSchema("movies")
	require.True(t, ok)
	require.Equal(t, moviesSchema, actualSchema)
}

func runServer(t *testing.T, clus cluster.Cluster) (*meta.Controller, *command.Executor) {
	t.Helper()

	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pushEngine := push.NewPushEngine(clus, shardr)
	pullEngine := pull.NewPullEngine(clus, metaController)
	ce := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus)
	clus.RegisterNotificationListener(cluster.NotificationTypeDDLStatement, ce)
	clus.RegisterNotificationListener(cluster.NotificationTypeCloseSession, pullEngine)
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	clus.RegisterShardListenerFactory(pushEngine)
	err := clus.Start()
	require.NoError(t, err)
	err = metaController.Start()
	require.NoError(t, err)
	err = pushEngine.Start()
	require.NoError(t, err)
	err = pullEngine.Start()
	require.NoError(t, err)
	err = shardr.Start()
	require.NoError(t, err)

	return metaController, ce
}
