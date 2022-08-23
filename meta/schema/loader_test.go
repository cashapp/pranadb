package schema

import (
	"context"
	"github.com/squareup/pranadb/cluster/fake"
	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/remoting"
	"testing"

	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/protolib"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
	"github.com/stretchr/testify/require"
)

func TestLoader(t *testing.T) {
	type ddl struct {
		schema  string
		queries []string
	}
	tests := []struct {
		name string
		ddl  []ddl
	}{
		{
			name: "sources",
			ddl: []ddl{{
				schema: "location",
				queries: []string{`create source location(id bigint, x varchar, y varchar, primary key (id) )
					with (
						brokername = "testbroker",
						topicname = "testtopic",
		              headerencoding = "json",
						keyencoding = "json",
						valueencoding = "json",
						columnselectors = (
							meta("key").k0,
							v1,
							v2
						),
						properties = (
							"prop1" = "val1",
							"prop2" = "val2"
						)
					)
					`},
			}, {
				schema: "hollywood",
				queries: []string{
					`create source actor(id bigint, name varchar, age int, primary key (id) )
					with (
						brokername = "testbroker",
						topicname = "testtopic",
		              headerencoding = "json",
						keyencoding = "json",
						valueencoding = "json",
						columnselectors = (
							meta("key").k0,
							v1,
							v2
						),
						properties = (
							"prop1" = "val1",
							"prop2" = "val2"
						)
					)
				`,
					`create source movies(id bigint, title varchar, director varchar, year int, primary key (id))
					with (
						brokername = "testbroker",
						topicname = "testtopic",
		              headerencoding = "json",
						keyencoding = "json",
						valueencoding = "json",
						columnselectors = (
							meta("key").k0,
							v1,
							v2,
							v3
						),
						properties = (
							"prop1" = "val1",
							"prop2" = "val2"
						)
					)`,
				},
			}},
		},
		{
			name: "mvs",
			ddl: []ddl{{
				schema: "hollywood",
				queries: []string{
					`create source movies(id bigint, title varchar, director varchar, year int, primary key (id))
                     with (
						brokername = "testbroker",
						topicname = "testtopic",
                        headerencoding = "json",
						keyencoding = "json",
						valueencoding = "json",
						columnselectors = (
							meta("key").k0,
							v1,
							v2,
							v3
						),
						properties = (
							"prop1" = "val1",
							"prop2" = "val2"
						)
					)`,
					`create materialized view latest_movies as
						select director,year
						from movies
						where year > 2020`,
				},
			}},
		},
	}
	for _, test := range tests {
		// nolint: scopelint
		t.Run(test.name, func(t *testing.T) {
			clus := fake.NewFakeCluster(1, 1)
			notifier := remoting.NewFakeServer()
			metaController, executor := runServer(t, clus, notifier)
			expectedSchemas := make(map[string]*common.Schema)
			for _, ddl := range test.ddl {
				numTables := 0
				schema := metaController.GetOrCreateSchema(ddl.schema)
				session := executor.CreateExecutionContext(context.Background(), schema)
				for _, query := range ddl.queries {
					_, err := executor.ExecuteSQLStatement(session, query, nil, nil)
					numTables++
					require.NoError(t, err)
				}
				require.Equal(t, schema.LenTables(), numTables)
				expectedSchemas[ddl.schema] = schema
			}

			// Restart the server
			_ = clus.Stop()
			metaController, executor = runServer(t, clus, notifier)
			_, ok := metaController.GetSchema("test")
			require.False(t, ok)

			loader := NewLoader(metaController, executor.GetPushEngine(), executor.GetPullEngine())
			require.NoError(t, loader.Start())

			for _, schema := range test.ddl {
				expected := expectedSchemas[schema.schema]
				actual, ok := metaController.GetSchema(schema.schema)
				require.True(t, ok)
				require.Equal(t, expected, actual)
			}
		})
	}
}

func runServer(t *testing.T, clus cluster.Cluster, notif *remoting.FakeServer) (*meta.Controller, *command.Executor) {
	t.Helper()
	fakeKafka := kafka.NewFakeKafka()
	_, err := fakeKafka.CreateTopic("testtopic", 10)
	require.NoError(t, err)
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pullEngine := pull.NewPullEngine(clus, metaController, shardr)
	config := conf.NewTestConfig(fakeKafka.ID)
	pushEngine := push.NewPushEngine(clus, shardr, metaController, config, pullEngine, protolib.EmptyRegistry, failinject.NewDummyInjector())
	ddlClient := remoting.NewFakeClient(notif)
	ddlResetClient := remoting.NewFakeClient(notif)
	ce := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus, ddlClient, ddlResetClient, protolib.EmptyRegistry, failinject.NewDummyInjector(), config)
	notif.RegisterMessageHandler(remoting.ClusterMessageDDLStatement, ce.DDlCommandRunner().DdlHandler())
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	clus.RegisterShardListenerFactory(pushEngine)
	err = clus.Start()
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
