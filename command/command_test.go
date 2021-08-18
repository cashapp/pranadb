package command

import (
	"fmt"
	"testing"

	"github.com/alecthomas/repr"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/push/source"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
)

// TODO these test seems to add little value - can be replaced with SQLTests which are also easier to maintain

func TestCommandExecutorExecuteStatement(t *testing.T) {
	fakeKafka := kafka.NewFakeKafka()
	_, err := fakeKafka.CreateTopic("testtopic", 10)
	require.NoError(t, err)

	clus := cluster.NewFakeCluster(1, 1)
	require.NoError(t, err)
	metaController := meta.NewController(clus)
	err = metaController.Start()
	require.NoError(t, err)
	shardr := sharder.NewSharder(clus)
	pullEngine := pull.NewPullEngine(clus, metaController)
	config := conf.NewTestConfig(fakeKafka.ID, zaptest.NewLogger(t))
	pushEngine := push.NewPushEngine(clus, shardr, metaController, config, pullEngine)
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	clus.RegisterShardListenerFactory(pushEngine)
	err = clus.Start()
	require.NoError(t, err)
	fakeNotifier := notifier.NewFakeNotifier()
	ce := NewCommandExecutor(metaController, pushEngine, pullEngine, clus, fakeNotifier)
	fakeNotifier.RegisterNotificationListener(notifier.NotificationTypeDDLStatement, ce)
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
			) with (
                brokername = "testbroker",
                topicname = "testtopic",
                headerencoding = "json",
                keyencoding = "json",
                valueencoding = "json",
                columnselectors = (
				    "k.k0",
                    "v.v1",
                    "v.v2"
				)
                properties = (
                    "prop1" = "val1",
                    "prop2" = "val2"
                )
            )
		`, sourceInfo: &common.SourceInfo{
			TableInfo: &common.TableInfo{
				ID:             common.UserTableIDBase,
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
			TopicInfo: &common.TopicInfo{
				BrokerName:     "testbroker",
				TopicName:      "testtopic",
				HeaderEncoding: common.EncodingJSON,
				KeyEncoding:    common.EncodingJSON,
				ValueEncoding:  common.EncodingJSON,
				ColSelectors: []string{
					"k.k0",
					"v.v1",
					"v.v2",
				},
				Properties: map[string]string{
					"prop1": "val1",
					"prop2": "val2",
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
				ID:             common.UserTableIDBase,
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
			executor, err := ce.ExecuteSQLStatement(s, test.query)
			require.NoError(t, err)
			actual, err := executor.GetRows(999)
			require.NoError(t, err)
			expected, err := test.rows.GetRows(999)
			require.NoError(t, err)
			require.Equal(t, expected, actual)

			if test.sourceInfo != nil {
				rf := common.NewRowsFactory(meta.TableDefTableInfo.ColumnTypes)
				row, err := table.LookupInPk(meta.TableDefTableInfo.TableInfo, []interface{}{int64(common.UserTableIDBase)}, meta.TableDefTableInfo.PrimaryKeyCols, cluster.SystemSchemaShardID, rf, clus)
				require.NoError(t, err)
				require.NotNil(t, row)
				require.Equal(t, test.sourceInfo, meta.DecodeSourceInfoRow(row))
			} else if test.mvInfo != nil {
				rf := common.NewRowsFactory(meta.TableDefTableInfo.ColumnTypes)
				row, err := table.LookupInPk(meta.TableDefTableInfo.TableInfo, []interface{}{int64(common.UserTableIDBase)}, meta.TableDefTableInfo.PrimaryKeyCols, cluster.SystemSchemaShardID, rf, clus)
				require.NoError(t, err)
				repr.Println(meta.DecodeMaterializedViewInfoRow(row))
				require.Equal(t, test.mvInfo, meta.DecodeMaterializedViewInfoRow(row))
			}
		})
	}
}

func TestCommandExecutorPrepareQuery(t *testing.T) {
	fakeKafka := kafka.NewFakeKafka()
	_, err := fakeKafka.CreateTopic("testtopic", 10)
	require.NoError(t, err)
	clus := cluster.NewFakeCluster(1, 1)
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pullEngine := pull.NewPullEngine(clus, metaController)
	config := conf.NewTestConfig(fakeKafka.ID, zaptest.NewLogger(t))
	pushEngine := push.NewPushEngine(clus, shardr, metaController, config, pullEngine)
	fakeNotifier := notifier.NewFakeNotifier()
	ce := NewCommandExecutor(metaController, pushEngine, pullEngine, clus, fakeNotifier)
	fakeNotifier.RegisterNotificationListener(notifier.NotificationTypeDDLStatement, ce)
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
	s := ce.CreateSession("test")

	sql := `
			create source sensor_readings(
				sensor_id bigint,
				location varchar,
				temperature double,
				primary key (sensor_id)
			)  with (
                brokername = "testbroker",
                topicname = "testtopic",
                headerencoding = "json",
                keyencoding = "json",
                valueencoding = "json",
                columnselectors = (
				    "k.k0",
                    "v.v1",
                    "v.v2"
				)
                properties = (
                    "prop1" = "val1",
                    "prop2" = "val2"
                )
            )
		`
	_, err = ce.ExecuteSQLStatement(s, sql)
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

	groupID := source.GenerateGroupID(config.ClusterID, src)
	err = kafka.IngestRows(fakeKafka, src, rows, groupID, &kafka.JSONKeyJSONValueEncoder{})
	require.NoError(t, err)

	err = pushEngine.WaitForProcessingToComplete()
	require.NoError(t, err)

	sql = "prepare select * from sensor_readings where location=?"
	prepExec, err := ce.ExecuteSQLStatement(s, sql)
	require.NoError(t, err)
	require.NotNil(t, prepExec)
	prepRows, err := prepExec.GetRows(1)
	require.NoError(t, err)
	require.Equal(t, 1, prepRows.RowCount())
	row := prepRows.GetRow(0)
	psID := row.GetInt64(0)

	ex, err := ce.ExecuteSQLStatement(s, fmt.Sprintf(`execute %d "wincanton"`, psID))
	require.NoError(t, err)
	rowsAct, err := ex.GetRows(100)
	require.NoError(t, err)
	require.Equal(t, 1, rowsAct.RowCount())
	act := rowsAct.GetRow(0)
	commontest.RowsEqual(t, rows.GetRow(1), act, colTypes)

	ex, err = ce.ExecuteSQLStatement(s, fmt.Sprintf(`execute %d "london"`, psID))
	require.NoError(t, err)
	rowsAct, err = ex.GetRows(100)
	require.NoError(t, err)
	require.Equal(t, 1, rowsAct.RowCount())
	act = rowsAct.GetRow(0)
	commontest.RowsEqual(t, rows.GetRow(0), act, colTypes)
}
