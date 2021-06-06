package sql

import (
	"context"
	"fmt"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	tidbTypes "github.com/pingcap/tidb/types"
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
func SchemasToInfoSchema(schemaManager SchemaManager) (infoschema.InfoSchema, error) {
	allSchemas := schemaManager.AllSchemas()
	return newPranaInfoSchema(allSchemas)
}

func convertColumnType(columnType ColumnType) (types.FieldType, error) {
	switch columnType {
	case TinyIntColumnType:
		return *types.NewFieldType(mysql.TypeTiny), nil
	case IntColumnType:
		return *types.NewFieldType(mysql.TypeLong), nil
	case BigIntColumnType:
		return *types.NewFieldType(mysql.TypeLonglong), nil
	case DoubleColumnType:
		return *types.NewFieldType(mysql.TypeDouble), nil
	case DecimalColumnType:
		return *types.NewFieldType(mysql.TypeNewDecimal), nil
	case VarcharColumnType:
		return *types.NewFieldType(mysql.TypeVarchar), nil
	case TimestampColumnType:
		return *types.NewFieldType(mysql.TypeTimestamp), nil
	default:
		return types.FieldType{}, fmt.Errorf("unknown colum type %d", columnType.typeNumber)
	}
}

func newPranaInfoSchema(schemaInfos []*SchemaInfo) (infoschema.InfoSchema, error) {
	result := &pranaInfoSchema{}
	result.schemaMap = make(map[string]*schemaTables)

	for _, schemaInfo := range schemaInfos {

		var tabInfos []*model.TableInfo
		tablesMap := make(map[string]table.Table)
		for _, tableInfo := range schemaInfo.tablesInfos {

			var columns []*model.ColumnInfo
			for _, columnInfo := range tableInfo.columnInfos {

				colType, err := convertColumnType(columnInfo.columnType)
				if err != nil {
					return nil, err
				}

				col := &model.ColumnInfo{
					State:     model.StatePublic,
					Offset:    columnInfo.columnIndex,
					Name:      model.NewCIStr(columnInfo.columnName),
					FieldType: colType,
					ID:        int64(columnInfo.columnIndex + 1),
				}

				columns = append(columns, col)
			}

			tab := &model.TableInfo{
				ID: tableInfo.id,
				Columns:    columns,
				Indices:    []*model.IndexInfo{}, // TODO indexes
				Name:       model.NewCIStr(tableInfo.tableName),
				PKIsHandle: true,
			}

			tablesMap[tableInfo.tableName] = newTiDBTable(tab)

			tabInfos = append(tabInfos, tab)
		}

		dbInfo := &model.DBInfo{ID: 0, Name: model.NewCIStr(schemaInfo.schemaName), Tables: tabInfos}

		tableNames := &schemaTables{
			dbInfo: dbInfo,
			tables: tablesMap,
		}
		result.schemaMap[schemaInfo.schemaName] = tableNames
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

func (pis* pranaInfoSchema) TableExists(schema, table model.CIStr) bool {
	if tbNames, ok := pis.schemaMap[schema.L]; ok {
		if _, ok = tbNames.tables[table.L]; ok {
			return true
		}
	}
	return false
}

func (pis* pranaInfoSchema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	for _, v := range pis.schemaMap {
		if v.dbInfo.ID == id {
			return v.dbInfo, true
		}
	}
	return nil, false
}

func (pis* pranaInfoSchema) SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
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

func (pis* pranaInfoSchema) AllSchemaNames() (names []string) {
	for _, v := range pis.schemaMap {
		names = append(names, v.dbInfo.Name.O)
	}
	return
}

func (pis* pranaInfoSchema) AllSchemas() (schemas []*model.DBInfo) {
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
