package sql

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	tidbTable "github.com/pingcap/tidb/table"
	tidbTypes "github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
)

// Implementation of TiDB InfoSchema so we can plug our schema into the TiDB planner
// Derived from the tIDB MockInfoSchema
// We only implement the parts we actually need
type pranaInfoSchema struct {
	schemaMap map[string]*schemaTables
}

type schemaTables struct {
	dbInfo *model.DBInfo
	tables map[string]tidbTable.Table
}

func NewPranaInfoSchema(schemaInfos []*SchemaInfo) (infoschema.InfoSchema, error) {
	result := &pranaInfoSchema{}
	result.schemaMap = make(map[string]*schemaTables)

	for _, schemaInfo := range schemaInfos {

		var tabInfos []*model.TableInfo
		tablesMap := make(map[string]tidbTable.Table)
		for _, tableInfo := range schemaInfo.TablesInfos {

			var columns []*model.ColumnInfo
			for columnIndex, columnType := range tableInfo.ColumnTypes {

				colType, err := ConvertPranaTypeToTiDBType(columnType)
				if err != nil {
					return nil, err
				}

				col := &model.ColumnInfo{
					State:     model.StatePublic,
					Offset:    columnIndex,
					Name:      model.NewCIStr(tableInfo.ColumnNames[columnIndex]),
					FieldType: *colType,
					ID:        int64(columnIndex + 1),
				}

				for pkIndex := range tableInfo.PrimaryKeyCols {
					if columnIndex == pkIndex {
						col.Flag |= mysql.PriKeyFlag
					}
				}

				columns = append(columns, col)
			}

			tableName := model.NewCIStr(tableInfo.TableName)

			var indexes []*model.IndexInfo
			var pkCols []*model.IndexColumn
			for columnIndex := range tableInfo.PrimaryKeyCols {
				col := &model.IndexColumn{
					Name:   model.NewCIStr(tableInfo.ColumnNames[columnIndex]),
					Offset: columnIndex,
					Length: 0,
				}

				pkCols = append(pkCols, col)
			}

			pkIndex := &model.IndexInfo{
				ID:        0,
				Name:      model.NewCIStr(fmt.Sprintf("PK_%s", tableInfo.TableName)),
				Table:     tableName,
				Columns:   pkCols,
				State:     model.StatePublic,
				Comment:   "",
				Tp:        model.IndexTypeHash,
				Unique:    true,
				Primary:   true,
				Invisible: false,
				Global:    false,
			}

			indexes = append(indexes, pkIndex)

			tab := &model.TableInfo{
				ID:         int64(tableInfo.ID),
				Columns:    columns,
				Indices:    indexes,
				Name:       tableName,
				PKIsHandle: len(tableInfo.PrimaryKeyCols) == 1,
				State:      model.StatePublic,
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

func (pis *pranaInfoSchema) TableByName(schema, table model.CIStr) (t tidbTable.Table, err error) {
	if tbNames, ok := pis.schemaMap[schema.L]; ok {
		if t, ok = tbNames.tables[table.L]; ok {
			return
		}
	}
	return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(schema, table)
}

func (pis *pranaInfoSchema) TableExists(schema, table model.CIStr) bool {
	if tbNames, ok := pis.schemaMap[schema.L]; ok {
		if _, ok = tbNames.tables[table.L]; ok {
			return true
		}
	}
	return false
}

func (pis *pranaInfoSchema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	for _, v := range pis.schemaMap {
		if v.dbInfo.ID == id {
			return v.dbInfo, true
		}
	}
	return nil, false
}

func (pis *pranaInfoSchema) SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
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

func (p pranaInfoSchema) TableByID(id int64) (tidbTable.Table, bool) {
	panic("should not be called")
}

func (p pranaInfoSchema) AllocByID(id int64) (autoid.Allocators, bool) {
	panic("should not be called")
}

func (pis *pranaInfoSchema) AllSchemaNames() (names []string) {
	for _, v := range pis.schemaMap {
		names = append(names, v.dbInfo.Name.O)
	}
	return
}

func (pis *pranaInfoSchema) AllSchemas() (schemas []*model.DBInfo) {
	for _, v := range pis.schemaMap {
		schemas = append(schemas, v.dbInfo)
	}
	return
}

func (p pranaInfoSchema) Clone() (result []*model.DBInfo) {
	panic("should not be called")
}

func (p pranaInfoSchema) SchemaTables(schema model.CIStr) []tidbTable.Table {
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

func (p pranaInfoSchema) FindTableByPartitionID(partitionID int64) (tidbTable.Table, *model.DBInfo, *model.PartitionDefinition) {
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
	columns   []*tidbTable.Column
	indexes   []tidbTable.Index
}

func newTiDBTable(tableInfo *model.TableInfo) *tiDBTable {
	var cols []*tidbTable.Column
	for _, colInfo := range tableInfo.Columns {
		cols = append(cols, &tidbTable.Column{
			ColumnInfo: colInfo,
		})
	}
	var indexes []tidbTable.Index
	for _, indexInfo := range tableInfo.Indices {
		indexes = append(indexes, newTiDBIndex(indexInfo))
	}
	tab := tiDBTable{
		tableInfo: tableInfo,
		columns:   cols,
		indexes:   indexes,
	}
	return &tab
}

func (t *tiDBTable) Cols() []*tidbTable.Column {
	return t.columns
}

func (t *tiDBTable) VisibleCols() []*tidbTable.Column {
	return t.columns
}

func (t *tiDBTable) HiddenCols() []*tidbTable.Column {
	return nil
}

func (t *tiDBTable) WritableCols() []*tidbTable.Column {
	return t.columns
}

func (t *tiDBTable) FullHiddenColsAndVisibleCols() []*tidbTable.Column {
	return t.columns
}

func (t *tiDBTable) Indices() []tidbTable.Index {
	return t.indexes
}

func (t *tiDBTable) RecordPrefix() kv.Key {
	panic("should not be called")
}

func (t tiDBTable) AddRecord(ctx sessionctx.Context, r []tidbTypes.Datum, opts ...tidbTable.AddRecordOption) (recordID kv.Handle, err error) {
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

func (t *tiDBTable) Type() tidbTable.Type {
	return tidbTable.NormalTable
}

type tiDBIndex struct {
	indexInfo *model.IndexInfo
}

func (t *tiDBIndex) Meta() *model.IndexInfo {
	return t.indexInfo
}

func (t tiDBIndex) Create(ctx sessionctx.Context, txn kv.Transaction, indexedValues []tidbTypes.Datum, h kv.Handle, handleRestoreData []tidbTypes.Datum, opts ...tidbTable.CreateIdxOptFunc) (kv.Handle, error) {
	panic("should not be called")
}

func (t tiDBIndex) Delete(sc *stmtctx.StatementContext, txn kv.Transaction, indexedValues []tidbTypes.Datum, h kv.Handle) error {
	panic("should not be called")
}

func (t tiDBIndex) Drop(txn kv.Transaction) error {
	panic("should not be called")
}

func (t tiDBIndex) Exist(sc *stmtctx.StatementContext, txn kv.Transaction, indexedValues []tidbTypes.Datum, h kv.Handle) (bool, kv.Handle, error) {
	panic("should not be called")
}

func (t tiDBIndex) GenIndexKey(sc *stmtctx.StatementContext, indexedValues []tidbTypes.Datum, h kv.Handle, buf []byte) (key []byte, distinct bool, err error) {
	panic("should not be called")
}

func (t tiDBIndex) Seek(sc *stmtctx.StatementContext, r kv.Retriever, indexedValues []tidbTypes.Datum) (iter tidbTable.IndexIterator, hit bool, err error) {
	panic("should not be called")
}

func (t tiDBIndex) SeekFirst(r kv.Retriever) (iter tidbTable.IndexIterator, err error) {
	panic("should not be called")
}

func (t tiDBIndex) FetchValues(row []tidbTypes.Datum, columns []tidbTypes.Datum) ([]tidbTypes.Datum, error) {
	panic("should not be called")
}

func newTiDBIndex(indexInfo *model.IndexInfo) *tiDBIndex {
	index := tiDBIndex{
		indexInfo: indexInfo,
	}
	return &index
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
	// We're not using TiKV so we're saying no you can't push anything to the KV store
	// However we might want to change this as we do actually want to compute near the data
	// and the planner can help us here
	return false
}

// This is needed for the TiDB planner
type fakeStorage struct {
	client kv.Client
}

func (f fakeStorage) Begin() (kv.Transaction, error) {
	panic("should not be called")
}

func (f fakeStorage) BeginWithOption(option tikv.StartTSOption) (kv.Transaction, error) {
	panic("should not be called")
}

func (f fakeStorage) GetSnapshot(ver kv.Version) kv.Snapshot {
	panic("should not be called")
}

func (f fakeStorage) GetClient() kv.Client {
	return f.client
}

func (f fakeStorage) GetMPPClient() kv.MPPClient {
	panic("should not be called")
}

func (f fakeStorage) Close() error {
	panic("should not be called")
}

func (f fakeStorage) UUID() string {
	panic("should not be called")
}

func (f fakeStorage) CurrentVersion(txnScope string) (kv.Version, error) {
	panic("should not be called")
}

func (f fakeStorage) GetOracle() oracle.Oracle {
	panic("should not be called")
}

func (f fakeStorage) SupportDeleteRange() (supported bool) {
	panic("should not be called")
}

func (f fakeStorage) Name() string {
	panic("should not be called")
}

func (f fakeStorage) Describe() string {
	panic("should not be called")
}

func (f fakeStorage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	panic("should not be called")
}

func (f fakeStorage) GetMemCache() kv.MemManager {
	panic("should not be called")
}

func (f fakeStorage) GetMinSafeTS(txnScope string) uint64 {
	panic("should not be called")
}

func (f fakeStorage) GetLockWaits() ([]*deadlock.WaitForEntry, error) {
	panic("should not be called")
}
