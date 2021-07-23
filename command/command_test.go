package command

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"testing"
	"time"

	"github.com/alecthomas/repr"
	"github.com/squareup/pranadb/table"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
)

func TestCommandExecutorExecuteStatement(t *testing.T) {
	clus := cluster.NewFakeCluster(1, 1)
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pushEngine := push.NewPushEngine(clus, shardr)
	pullEngine := pull.NewPullEngine(clus, metaController)
	ce := NewCommandExecutor(metaController, pushEngine, pullEngine, clus)
	clus.RegisterNotificationListener(cluster.NotificationTypeDDLStatement, ce)
	s := ce.CreateSession("test")

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
			TableInfo: &common.TableInfo{
				ID:             100,
				SchemaName:     "test",
				Name:           "sensor_readings",
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
		`, mvInfo: &common.MaterializedViewInfo{
			TableInfo: &common.TableInfo{
				ID:             100,
				SchemaName:     "test",
				Name:           "sensor_readings",
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
				row, err := table.LookupInPk(meta.SchemaTableInfo.TableInfo, []interface{}{int64(common.UserTableIDBase)}, meta.SchemaTableInfo.PrimaryKeyCols, cluster.SchemaTableShardID, rf, clus)
				require.NoError(t, err)
				require.NotNil(t, row)
				require.Equal(t, test.sourceInfo, meta.DecodeSourceInfoRow(row))
			} else if test.mvInfo != nil {
				rf := common.NewRowsFactory(meta.SchemaTableInfo.ColumnTypes)
				row, err := table.LookupInPk(meta.SchemaTableInfo.TableInfo, []interface{}{int64(common.UserTableIDBase)}, meta.SchemaTableInfo.PrimaryKeyCols, cluster.SchemaTableShardID, rf, clus)
				require.NoError(t, err)
				repr.Println(meta.DecodeMaterializedViewInfoRow(row))
				require.Equal(t, test.mvInfo, meta.DecodeMaterializedViewInfoRow(row))
			}
		})
	}
}

func TestCommandExecutorPrepareQuery(t *testing.T) {
	clus := cluster.NewFakeCluster(1, 1)
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pushEngine := push.NewPushEngine(clus, shardr)
	pullEngine := pull.NewPullEngine(clus, metaController)
	ce := NewCommandExecutor(metaController, pushEngine, pullEngine, clus)
	clus.RegisterNotificationListener(cluster.NotificationTypeDDLStatement, ce)
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
	s := ce.CreateSession("test")

	seqGenerator := &preallocSeqGen{sequences: []uint64{1, 2}}
	sql := `
			create source sensor_readings(
				sensor_id bigint,
				location varchar,
				temperature double,
				primary key (sensor_id)
			)
		`
	_, err = ce.executeSQLStatementInternal(s, sql, true, seqGenerator)
	require.NoError(t, err)

	src, ok := metaController.GetSource("test", "sensor_readings")
	require.True(t, ok)

	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}
	rf := common.NewRowsFactory(colTypes)
	rows := rf.NewRows(1)
	rows.AppendInt64ToColumn(0, 1)
	rows.AppendStringToColumn(1, "london")
	rows.AppendFloat64ToColumn(2, 23.1)

	rows.AppendInt64ToColumn(0, 2)
	rows.AppendStringToColumn(1, "wincanton")
	rows.AppendFloat64ToColumn(2, 32.1)

	err = pushEngine.IngestRows(rows, src.TableInfo.ID)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	sql = "prepare select * from sensor_readings where location=?"
	prepExec, err := ce.ExecuteSQLStatement(s, sql)
	require.NoError(t, err)
	require.NotNil(t, prepExec)
	prepRows, err := prepExec.GetRows(1)
	require.NoError(t, err)
	require.Equal(t, 1, prepRows.RowCount())
	row := prepRows.GetRow(0)
	psID := row.GetInt64(0)

	ex, err := ce.ExecuteSQLStatement(s, fmt.Sprintf("execute %d wincanton", psID))
	require.NoError(t, err)
	rowsAct, err := ex.GetRows(100)
	require.NoError(t, err)
	require.Equal(t, 1, rowsAct.RowCount())
	act := rowsAct.GetRow(0)
	commontest.RowsEqual(t, rows.GetRow(1), act, colTypes)

	ex, err = ce.ExecuteSQLStatement(s, fmt.Sprintf("execute %d london", psID))
	require.NoError(t, err)
	rowsAct, err = ex.GetRows(100)
	require.NoError(t, err)
	require.Equal(t, 1, rowsAct.RowCount())
	act = rowsAct.GetRow(0)
	commontest.RowsEqual(t, rows.GetRow(0), act, colTypes)
}
