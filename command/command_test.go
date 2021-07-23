package command

import (
	"testing"

	"github.com/alecthomas/repr"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/sess"
	"github.com/squareup/pranadb/table"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
)

func TestCommandExecutorExecutePullQuery(t *testing.T) {
	clus := cluster.NewFakeCluster(1, 1)
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pushEngine := push.NewPushEngine(clus, shardr)
	pullEngine := pull.NewPullEngine(clus, metaController)
	ce := NewCommandExecutor(metaController, pushEngine, pullEngine, clus)
	clus.RegisterNotificationListener(cluster.NotificationTypeDDLStatement, ce)
	schema := metaController.GetOrCreateSchema("test")
	s := sess.NewSession(schema, parplan.NewPlanner())

	tests := []struct {
		name       string
		query      string
		sourceInfo *common.SourceInfo
		mvInfo     *common.MaterializedViewInfo
		rows       exec.PullExecutor
	}{
		{name: "CreateSource", query: `
			create source sensor_readings(
				sensor_id bigint,
				location varchar,
				temperature double,
				primary key (sensor_id)
			)
		`, sourceInfo: &common.SourceInfo{
			SchemaName: "test",
			Name:       "sensor_readings",
			TableInfo: &common.TableInfo{
				ID:             100,
				TableName:      "sensor_readings",
				PrimaryKeyCols: []int{0},
				ColumnNames:    []string{"sensor_id", "location", "temperature"},
				ColumnTypes: []common.ColumnType{
					{Type: common.TypeBigInt},
					{Type: common.TypeVarchar},
					{Type: common.TypeDouble},
				},
			},
		}, rows: exec.Empty},
		{name: "CreateMV", query: `
			create materialized view test as
				select sensor_id, max(temperature)
				from test.sensor_readings
				where location='wincanton' group by sensor_id
		`, mvInfo: &common.MaterializedViewInfo{SchemaName: "test",
			Name: "sensor_readings",
			TableInfo: &common.TableInfo{
				ID:             100,
				TableName:      "sensor_readings",
				PrimaryKeyCols: []int{0},
				ColumnNames:    []string{"sensor_id", "location", "temperature"},
				ColumnTypes: []common.ColumnType{
					{Type: common.TypeBigInt},
					{Type: common.TypeVarchar},
					{Type: common.TypeDouble},
				},
			},
		}, rows: exec.Empty},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seqGenerator := &preallocSeqGen{sequences: []uint64{1, 2}}
			executor, err := ce.executeSQLStatementInternal(s, test.query, true, seqGenerator)
			require.NoError(t, err)
			actual, err := executor.GetRows(999)
			require.NoError(t, err)
			expected, err := test.rows.GetRows(999)
			require.NoError(t, err)
			require.Equal(t, expected, actual)

			if test.sourceInfo != nil {
				rf := common.NewRowsFactory(meta.SchemaTableInfo.ColumnTypes)
				row, err := table.LookupInPk(meta.SchemaTableInfo, []interface{}{int64(common.UserTableIDBase)}, meta.SchemaTableInfo.PrimaryKeyCols, cluster.SchemaTableShardID, rf, clus)
				require.NoError(t, err)
				require.NotNil(t, row)
				require.Equal(t, test.sourceInfo, meta.DecodeSourceInfoRow(row))
			} else if test.mvInfo != nil {
				rf := common.NewRowsFactory(meta.SchemaTableInfo.ColumnTypes)
				row, err := table.LookupInPk(meta.SchemaTableInfo, []interface{}{int64(common.UserTableIDBase)}, meta.SchemaTableInfo.PrimaryKeyCols, cluster.SchemaTableShardID, rf, clus)
				require.NoError(t, err)
				repr.Println(meta.DecodeMaterializedViewInfoRow(row))
				require.Equal(t, test.mvInfo, meta.DecodeMaterializedViewInfoRow(row))
			}
		})
	}
}
