package planner

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
	tidbTypes "github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/squareup/pranadb/sql"
)

// Implementation of TiDB InfoSchema so we can plug our schema into the TiDB planner
// Derived from the tIDB MockInfoSchema
// We only implement the parts we actually need
type pranaInfoSchema struct {
	schemaMap map[string]*schemaTables
}

type schemaTables struct {
	dbInfo *model.DBInfo
	tables map[string]table.Table
}

// SchemasToInfoSchema converts Prana Schemas to InfoSchema which the TiDB planner understands
func SchemasToInfoSchema(schemaManager sql.SchemaManager) (infoschema.InfoSchema, error) {
	allSchemas := schemaManager.AllSchemas()
	return newPranaInfoSchema(allSchemas)
}

func ConvertColumnType(columnType *sql.ColumnType) (*types.FieldType, error) {
	switch *columnType {
	case sql.TinyIntColumnType:
		return types.NewFieldType(mysql.TypeTiny), nil
	case sql.IntColumnType:
		return types.NewFieldType(mysql.TypeLong), nil
	case sql.BigIntColumnType:
		return types.NewFieldType(mysql.TypeLonglong), nil
	case sql.DoubleColumnType:
		return types.NewFieldType(mysql.TypeDouble), nil
	case sql.DecimalColumnType:
		return types.NewFieldType(mysql.TypeNewDecimal), nil
	case sql.VarcharColumnType:
		return types.NewFieldType(mysql.TypeVarchar), nil
	case sql.TimestampColumnType:
		return types.NewFieldType(mysql.TypeTimestamp), nil
	default:
		return nil, fmt.Errorf("unknown colum type %d", columnType.TypeNumber)
	}
}

func newPranaInfoSchema(schemaInfos []*sql.SchemaInfo) (infoschema.InfoSchema, error) {
	result := &pranaInfoSchema{}
	result.schemaMap = make(map[string]*schemaTables)

	for _, schemaInfo := range schemaInfos {

		var tabInfos []*model.TableInfo
		tablesMap := make(map[string]table.Table)
		for _, tableInfo := range schemaInfo.TablesInfos {

			var columns []*model.ColumnInfo
			for _, columnInfo := range tableInfo.ColumnInfos {

				colType, err := ConvertColumnType(&columnInfo.ColumnType)
				if err != nil {
					return nil, err
				}

				col := &model.ColumnInfo{
					State:     model.StatePublic,
					Offset:    columnInfo.ColumnIndex,
					Name:      model.NewCIStr(columnInfo.ColumnName),
					FieldType: *colType,
					ID:        int64(columnInfo.ColumnIndex + 1),
				}

				columns = append(columns, col)
			}

			tab := &model.TableInfo{
				ID: tableInfo.ID,
				Columns:    columns,
				Indices:    []*model.IndexInfo{}, // TODO indexes
				Name:       model.NewCIStr(tableInfo.TableName),
				PKIsHandle: true,
			}

			tablesMap[tableInfo.TableName] = newTiDBTable(tab)

			tabInfos = append(tabInfos, tab)
		}

		dbInfo := &model.DBInfo{ID: 0, Name: model.NewCIStr(schemaInfo.SchemaName), Tables: tabInfos}

		tableNames := &schemaTables{
			dbInfo: dbInfo,
			tables: tablesMap,
		}
		result.schemaMap[schemaInfo.SchemaName] = tableNames
	}

	return result, nil
}


func (pis *pranaInfoSchema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	tableNames, ok := pis.schemaMap[schema.L]
	if !ok {
		return
	}
	return tableNames.dbInfo, true
}

func (pis *pranaInfoSchema) SchemaExists(schema model.CIStr) bool {
	_, ok := pis.schemaMap[schema.L]
	return ok
}

func (pis *pranaInfoSchema) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	if tbNames, ok := pis.schemaMap[schema.L]; ok {
		if t, ok = tbNames.tables[table.L]; ok {
			return
		}
	}
	return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(schema, table)
}

func (pis*pranaInfoSchema) TableExists(schema, table model.CIStr) bool {
	if tbNames, ok := pis.schemaMap[schema.L]; ok {
		if _, ok = tbNames.tables[table.L]; ok {
			return true
		}
	}
	return false
}

func (pis*pranaInfoSchema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	for _, v := range pis.schemaMap {
		if v.dbInfo.ID == id {
			return v.dbInfo, true
		}
	}
	return nil, false
}

func (pis*pranaInfoSchema) SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
	if tableInfo == nil {
		return nil, false
	}
	for _, v := range pis.schemaMap {
		if tbl, ok := v.tables[tableInfo.Name.L]; ok {
			if tbl.Meta().ID == tableInfo.ID {
				return v.dbInfo, true
			}
		}
	}
	return nil, false
}

func (p pranaInfoSchema) TableByID(id int64) (table.Table, bool) {
	panic("should not be called")
}

func (p pranaInfoSchema) AllocByID(id int64) (autoid.Allocators, bool) {
	panic("should not be called")
}

func (pis*pranaInfoSchema) AllSchemaNames() (names []string) {
	for _, v := range pis.schemaMap {
		names = append(names, v.dbInfo.Name.O)
	}
	return
}

func (pis*pranaInfoSchema) AllSchemas() (schemas []*model.DBInfo) {
	for _, v := range pis.schemaMap {
		schemas = append(schemas, v.dbInfo)
	}
	return
}

func (p pranaInfoSchema) Clone() (result []*model.DBInfo) {
	panic("should not be called")
}

func (p pranaInfoSchema) SchemaTables(schema model.CIStr) []table.Table {
	panic("should not be called")
}

func (p pranaInfoSchema) SchemaMetaVersion() int64 {
	return 0
}

func (p pranaInfoSchema) TableIsView(schema, table model.CIStr) bool {
	return false
}

func (p pranaInfoSchema) TableIsSequence(schema, table model.CIStr) bool {
	return false
}

func (p pranaInfoSchema) FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition) {
	panic("should not be called")
}

func (p pranaInfoSchema) BundleByName(name string) (*placement.Bundle, bool) {
	panic("should not be called")
}

func (p pranaInfoSchema) SetBundle(bundle *placement.Bundle) {
	panic("should not be called")
}

func (p pranaInfoSchema) RuleBundles() []*placement.Bundle {
	panic("should not be called")
}

type tiDBTable struct {
	tableInfo *model.TableInfo
	columns []*table.Column
}

func newTiDBTable(tableInfo *model.TableInfo) *tiDBTable {
	var cols []*table.Column
	for _, colInfo := range tableInfo.Columns {
		cols = append(cols, &table.Column{
			ColumnInfo:    colInfo,
		})
	}
	tab := tiDBTable{
		tableInfo: tableInfo,
		columns: cols,
	}
	return &tab
}

func (t *tiDBTable) Cols() []*table.Column {
	return t.columns
}

func (t *tiDBTable) VisibleCols() []*table.Column {
	return t.columns
}

func (t *tiDBTable) HiddenCols() []*table.Column {
	return nil
}

func (t *tiDBTable) WritableCols() []*table.Column {
	return t.columns
}

func (t *tiDBTable) FullHiddenColsAndVisibleCols() []*table.Column {
	return t.columns
}

func (t *tiDBTable) Indices() []table.Index {
	return nil
}

func (t *tiDBTable) RecordPrefix() kv.Key {
	panic("should not be called")
}

func (t tiDBTable) AddRecord(ctx sessionctx.Context, r []tidbTypes.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	panic("should not be called")
}

func (t tiDBTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []tidbTypes.Datum, touched []bool) error {
	panic("should not be called")
}

func (t tiDBTable) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []tidbTypes.Datum) error {
	panic("should not be called")
}

func (t tiDBTable) Allocators(ctx sessionctx.Context) autoid.Allocators {
	panic("should not be called")
}

func (t tiDBTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, allocIDs bool, tp autoid.AllocatorType) error {
	panic("should not be called")
}

func (t *tiDBTable) Meta() *model.TableInfo {
	return t.tableInfo
}

func (t *tiDBTable) Type() table.Type {
	return table.NormalTable
}


func NewSessionContext() sessionctx.Context {
	sessCtx := mock.NewContext()
	kvClient := fakeKVClient{}
	storage := fakeStorage{client: kvClient}
	d := domain.NewDomain(storage, 0, 0, 0, nil)
	domain.BindDomain(sessCtx, d)
	sessCtx.Store = storage
	return sessCtx
}

type fakeKVClient struct {
}

func (f fakeKVClient) Send(ctx context.Context, req *kv.Request, vars interface{}, sessionMemTracker *memory.Tracker, enabledRateLimitAction bool) kv.Response {
	panic("should not be called")
}

func (f fakeKVClient) IsRequestTypeSupported(reqType, subType int64) bool {
	return true
}

// This is needed for the TiDB planner
type fakeStorage struct {
	client kv.Client
}

func (f fakeStorage) Begin() (kv.Transaction, error) {
	panic("implement me")
}

func (f fakeStorage) BeginWithOption(option tikv.StartTSOption) (kv.Transaction, error) {
	panic("implement me")
}

func (f fakeStorage) GetSnapshot(ver kv.Version) kv.Snapshot {
	panic("implement me")
}

func (f fakeStorage) GetClient() kv.Client {
	return f.client
}

func (f fakeStorage) GetMPPClient() kv.MPPClient {
	panic("implement me")
}

func (f fakeStorage) Close() error {
	panic("implement me")
}

func (f fakeStorage) UUID() string {
	panic("implement me")
}

func (f fakeStorage) CurrentVersion(txnScope string) (kv.Version, error) {
	panic("implement me")
}

func (f fakeStorage) GetOracle() oracle.Oracle {
	panic("implement me")
}

func (f fakeStorage) SupportDeleteRange() (supported bool) {
	panic("implement me")
}

func (f fakeStorage) Name() string {
	panic("implement me")
}

func (f fakeStorage) Describe() string {
	panic("implement me")
}

func (f fakeStorage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	panic("implement me")
}

func (f fakeStorage) GetMemCache() kv.MemManager {
	panic("implement me")
}

func (f fakeStorage) GetMinSafeTS(txnScope string) uint64 {
	panic("implement me")
}

func (f fakeStorage) GetLockWaits() ([]*deadlock.WaitForEntry, error) {
	panic("implement me")
}
