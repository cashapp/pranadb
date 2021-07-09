package command

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/storage"
)

func TestCommandExecutorExecutePullQuery(t *testing.T) {
	cluster := cluster.NewFakeClusterManager(1, 1)
	store := storage.NewFakeStorage()
	metaController := meta.NewController(store)
	planner := parplan.NewPlanner()
	shardr := sharder.NewSharder(cluster)
	pushEngine := push.NewPushEngine(store, cluster, planner, shardr)
	pullEngine := pull.NewPullEngine(planner, store, cluster, metaController)
	ce := NewCommandExecutor(store, metaController, pushEngine, pullEngine, cluster)

	tests := []struct {
		name  string
		query string
		rows  exec.PullExecutor
	}{
		{name: "CreateSource", query: `
			create source sensor_readings(
				sensor_id big int,
				location varchar(255),
				temperature double,
				primary key (sensor_id)
			)
		`, rows: exec.Empty},
		{name: "CreateMV", query: `
			create materialized view test
				select sensor_id, max(temperature)
				from test.sensor_readings
				where location='wincanton' group by sensor_id
		`, rows: exec.Empty},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor, err := ce.ExecuteSQLStatement("test", test.query)
			require.NoError(t, err)
			actual, err := executor.GetRows(999)
			require.NoError(t, err)
			expected, err := test.rows.GetRows(999)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}
