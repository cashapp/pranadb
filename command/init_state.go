package command

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"strings"
)

func validateInitState(initTable string, thisState *common.TableInfo, metaController *meta.Controller) error {
	tabInfo, err := getInitialiseFromTable(thisState.SchemaName, initTable, metaController)
	if err != nil {
		return err
	}
	// The structure of the initial state table must be the same as this source
	matches := false
	if len(tabInfo.ColumnTypes) == len(thisState.ColumnTypes) {
		for i, thisColType := range thisState.ColumnTypes {
			if thisColType != tabInfo.ColumnTypes[i] {
				break
			}
			if i == len(thisState.ColumnTypes) - 1 {
				matches = true
			}
		}
	}
	if !matches {
		return errors.NewPranaErrorf(errors.InvalidStatement, structureNotMatchMsg(tabInfo, thisState))
	}
	return nil
}


func getInitialiseFromTable(schemaName string, tabName string, metaController *meta.Controller) (*common.TableInfo, error) {
	var tabInfo *common.TableInfo
	sInfo, ok := metaController.GetSource(schemaName, tabName)
	if !ok {
		mvInfo, ok := metaController.GetMaterializedView(schemaName, tabName)
		if !ok {
			return nil, errors.NewPranaErrorf(errors.UnknownTable, "Unknown table or materialized view %s.%s", schemaName, tabName)
		}
		tabInfo = mvInfo.TableInfo
	} else {
		tabInfo = sInfo.TableInfo
	}
	return tabInfo, nil
}

func structureNotMatchMsg(initialStateStructure *common.TableInfo, sourceStructure *common.TableInfo) string {
	var initTypes []string
	for _, colType := range initialStateStructure.ColumnTypes {
		initTypes = append(initTypes, colType.String())
	}
	var sourceTypes []string
	for _, colType := range sourceStructure.ColumnTypes {
		sourceTypes = append(sourceTypes, colType.String())
	}
	return fmt.Sprintf("initialState table structure does not match. initial structure: (%s) source structure: (%s)",
		strings.Join(initTypes, ","), strings.Join(sourceTypes, ","))
}


