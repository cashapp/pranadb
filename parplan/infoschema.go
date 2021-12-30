package parplan

import (
	"fmt"
	"github.com/squareup/pranadb/tidb"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/tidb/infoschema"
	tidbTable "github.com/squareup/pranadb/tidb/table"
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

type iSSchemaInfo struct {
	SchemaName  string
	TablesInfos map[string]*common.TableInfo
}

func schemaToInfoSchema(schema *common.Schema) infoschema.InfoSchema {

	tableInfos := schema.GetAllTableInfos()
	schemaInfo := iSSchemaInfo{
		SchemaName:  schema.Name,
		TablesInfos: tableInfos,
	}

	result := &pranaInfoSchema{}
	result.schemaMap = make(map[string]*schemaTables)

	var tabInfos []*model.TableInfo
	tablesMap := make(map[string]tidbTable.Table)
	for _, tableInfo := range schemaInfo.TablesInfos {
		if tableInfo.Internal {
			continue
		}

		var columns []*model.ColumnInfo
		for columnIndex, columnType := range tableInfo.ColumnTypes {
			if tableInfo.ColsVisible != nil && !tableInfo.ColsVisible[columnIndex] {
				continue
			}
			colType := common.ConvertPranaTypeToTiDBType(columnType)
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
		tableName := model.NewCIStr(tableInfo.Name)

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
			ID:        1001,
			Name:      model.NewCIStr(fmt.Sprintf("PK_%s", tableInfo.Name)),
			Table:     tableName,
			Columns:   pkCols,
			State:     model.StatePublic,
			Comment:   "",
			Tp:        model.IndexTypeBtree,
			Unique:    true,
			Primary:   true,
			Invisible: false,
			Global:    false,
		}

		indexes = append(indexes, pkIndex)

		if tableInfo.IndexInfos != nil {
			for _, indexInfo := range tableInfo.IndexInfos {
				var indexCols []*model.IndexColumn
				for columnIndex := range indexInfo.IndexCols {
					col := &model.IndexColumn{
						Name:   model.NewCIStr(tableInfo.ColumnNames[columnIndex]),
						Offset: columnIndex,
						Length: 0,
					}

					indexCols = append(indexCols, col)
				}
				index := &model.IndexInfo{
					ID:        int64(indexInfo.ID),
					Name:      model.NewCIStr(fmt.Sprintf("%s_%s", tableInfo.Name, indexInfo.Name)),
					Table:     tableName,
					Columns:   indexCols,
					State:     model.StatePublic,
					Comment:   "",
					Tp:        model.IndexTypeBtree,
					Unique:    false,
					Primary:   false,
					Invisible: false,
					Global:    false,
				}
				indexes = append(indexes, index)
			}
		}

		tab := &model.TableInfo{
			ID:         int64(tableInfo.ID),
			Columns:    columns,
			Indices:    indexes,
			Name:       tableName,
			PKIsHandle: len(tableInfo.PrimaryKeyCols) == 1,
			State:      model.StatePublic,
		}

		tablesMap[tableInfo.Name] = newTiDBTable(tab)

		tabInfos = append(tabInfos, tab)
	}

	dbInfo := &model.DBInfo{ID: 0, Name: model.NewCIStr(schemaInfo.SchemaName), Tables: tabInfos}

	tableNames := &schemaTables{
		dbInfo: dbInfo,
		tables: tablesMap,
	}
	result.schemaMap[schemaInfo.SchemaName] = tableNames

	return result
}

/*
type InfoSchema interface {
	SchemaByName(schema model.CIStr) (*model.DBInfo, bool)
	TableByName(schema, table model.CIStr) (table.Table, error)
	TableByID(id int64) (table.Table, bool)
	SchemaMetaVersion() int64
}
*/

func (pis *pranaInfoSchema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	tableNames, ok := pis.schemaMap[schema.L]
	if !ok {
		return
	}
	return tableNames.dbInfo, true
}

func (pis *pranaInfoSchema) TableByName(schema, table model.CIStr) (t tidbTable.Table, err error) {
	if tbNames, ok := pis.schemaMap[schema.L]; ok {
		if t, ok = tbNames.tables[table.L]; ok {
			return
		}
	}
	return nil, tidb.ErrTableNotExists.GenWithStackByArgs(schema, table)
}

func (pis pranaInfoSchema) TableByID(id int64) (tidbTable.Table, bool) {
	panic("should not be called")
}

func (pis *pranaInfoSchema) SchemaMetaVersion() int64 {
	return 0
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

func (t *tiDBTable) WritableCols() []*tidbTable.Column {
	return t.columns
}

func (t *tiDBTable) FullHiddenColsAndVisibleCols() []*tidbTable.Column {
	return t.columns
}

func (t *tiDBTable) Meta() *model.TableInfo {
	return t.tableInfo
}

type tiDBIndex struct {
	indexInfo *model.IndexInfo
}

func (t *tiDBIndex) Meta() *model.IndexInfo {
	return t.indexInfo
}

func newTiDBIndex(indexInfo *model.IndexInfo) *tiDBIndex {
	index := tiDBIndex{
		indexInfo: indexInfo,
	}
	return &index
}
