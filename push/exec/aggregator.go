package exec

import (
	"fmt"
	"github.com/squareup/pranadb/aggfuncs"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/table"
)

type Aggregator struct {
	pushExecutorBase
	aggFuncs            []aggfuncs.AggregateFunction
	PartialAggTableInfo *common.TableInfo
	FullAggTableInfo    *common.TableInfo
	groupByCols         []int // The group by column indexes in the child
	storage             cluster.Cluster
	sharder             *sharder.Sharder
}

type AggregateFunctionInfo struct {
	FuncType   aggfuncs.AggFunctionType
	Distinct   bool
	ArgExpr    *common.Expression
	ReturnType common.ColumnType
}

type aggStateHolder struct {
	aggState        *aggfuncs.AggState
	initialRowBytes []byte
	keyBytes        []byte
	rowBytes        []byte
	initialRow      *common.Row
	row             *common.Row
}

func NewAggregator(pkCols []int, aggFunctions []*AggregateFunctionInfo, partialAggTableInfo *common.TableInfo,
	fullAggTableInfo *common.TableInfo, groupByCols []int, storage cluster.Cluster, sharder *sharder.Sharder) (*Aggregator, error) {

	colTypes := make([]common.ColumnType, len(aggFunctions))
	for i, aggFunc := range aggFunctions {
		colTypes[i] = aggFunc.ReturnType
	}
	partialAggTableInfo.ColumnTypes = colTypes
	fullAggTableInfo.ColumnTypes = colTypes
	rf := common.NewRowsFactory(colTypes)
	pushBase := pushExecutorBase{
		colTypes:    colTypes,
		keyCols:     pkCols,
		rowsFactory: rf,
	}
	aggFuncs, err := createAggFunctions(aggFunctions, colTypes)
	if err != nil {
		return nil, err
	}
	return &Aggregator{
		pushExecutorBase:    pushBase,
		aggFuncs:            aggFuncs,
		PartialAggTableInfo: partialAggTableInfo,
		FullAggTableInfo:    fullAggTableInfo,
		groupByCols:         groupByCols,
		storage:             storage,
		sharder:             sharder,
	}, nil
}

func (a *Aggregator) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {

	// We first calculate the partial aggregations locally
	stateHolders := make(map[string]*aggStateHolder)
	numRows := rowsBatch.Len()
	readRows := a.rowsFactory.NewRows(numRows)
	for i := 0; i < numRows; i++ {
		row := rowsBatch.CurrentRow(i)
		if err := a.calcPartialAggregations(row, readRows, stateHolders, ctx.WriteBatch.ShardID); err != nil {
			return err
		}
	}

	// Store the results locally
	if err := a.storeAggregateResults(stateHolders, ctx.WriteBatch); err != nil {
		return err
	}

	// We send the partial aggregation results to the shard that owns the key
	for _, stateHolder := range stateHolders {
		if stateHolder.aggState.IsChanged() {
			// We ignore the first 16 bytes as this is shard-id|table-id
			remoteShardID, err := a.sharder.CalculateShard(sharder.ShardTypeHash, stateHolder.keyBytes[16:])
			if err != nil {
				return err
			}
			if err := ctx.Mover.QueueForRemoteSend(remoteShardID, stateHolder.initialRowBytes, stateHolder.rowBytes,
				ctx.WriteBatch.ShardID, a.FullAggTableInfo.ID, ctx.WriteBatch); err != nil {
				return err
			}
		}
	}
	return nil
}

// HandleRemoteRows is called when partial aggregation is forwarded from another shard
func (a *Aggregator) HandleRemoteRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {

	numRows := rowsBatch.Len()
	stateHolders := make(map[string]*aggStateHolder)
	readRows := a.rowsFactory.NewRows(numRows)
	numCols := len(a.colTypes)
	for i := 0; i < numRows; i++ {
		prevRow := rowsBatch.PreviousRow(i)
		currRow := rowsBatch.CurrentRow(i)
		if err := a.calcFullAggregation(prevRow, currRow, readRows, stateHolders, ctx.WriteBatch.ShardID, numCols); err != nil {
			return err
		}
	}

	// Store the results
	if err := a.storeAggregateResults(stateHolders, ctx.WriteBatch); err != nil {
		return err
	}

	resultRows := a.rowsFactory.NewRows(numRows)
	entries := make([]RowsEntry, 0, numRows)
	rc := 0

	// Send the rows to the parent
	for _, stateHolder := range stateHolders {
		if stateHolder.aggState.IsChanged() {
			prevRow := stateHolder.initialRow
			currRow := stateHolder.row
			pi := -1
			if prevRow != nil {
				resultRows.AppendRow(*prevRow)
				pi = rc
				rc++
			}
			ci := -1
			if currRow != nil {
				resultRows.AppendRow(*currRow)
				ci = rc
				rc++
			}
			entries = append(entries, NewRowsEntry(pi, ci))
		}
	}

	return a.parent.HandleRows(NewRowsBatch(resultRows, entries), ctx)
}

func (a *Aggregator) calcPartialAggregations(row *common.Row, readRows *common.Rows, aggStateHolders map[string]*aggStateHolder, shardID uint64) error {

	// Create the key
	keyBytes, err := a.createKey(row, shardID, a.GetChildren()[0].ColTypes(), a.groupByCols, a.PartialAggTableInfo.ID)
	if err != nil {
		return err
	}

	// Lookup existing aggregate state
	stateHolder, err := a.loadAggregateState(keyBytes, readRows, aggStateHolders)
	if err != nil {
		return err
	}

	// Evaluate the agg functions on the state
	return a.evaluateAggFunctions(stateHolder.aggState, row)
}

func (a *Aggregator) calcFullAggregation(prevRow *common.Row, currRow *common.Row, readRows *common.Rows,
	stateHolders map[string]*aggStateHolder, shardID uint64, numCols int) error {

	key, err := a.createKey(currRow, shardID, a.colTypes, a.keyCols, a.FullAggTableInfo.ID)
	if err != nil {
		return err
	}
	stateHolder, err := a.loadAggregateState(key, readRows, stateHolders)
	if err != nil {
		return err
	}

	var prevMergeState *aggfuncs.AggState
	if prevRow != nil {
		prevMergeState = aggfuncs.NewAggState(numCols)
		if err := a.initAggStateWithRow(prevRow, prevMergeState, numCols); err != nil {
			return err
		}
	}
	var currMergeState *aggfuncs.AggState
	if currRow != nil {
		currMergeState = aggfuncs.NewAggState(numCols)
		if err := a.initAggStateWithRow(currRow, currMergeState, numCols); err != nil {
			return err
		}
	}

	currAggState := stateHolder.aggState
	for index, aggFunc := range a.aggFuncs {
		switch aggFunc.ValueType().Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			if err := aggFunc.MergeInt64(prevMergeState, currMergeState, currAggState, index); err != nil {
				return err
			}
		case common.TypeDecimal:
			if err := aggFunc.MergeDecimal(prevMergeState, currMergeState, currAggState, index); err != nil {
				return err
			}
		case common.TypeDouble:
			if err := aggFunc.MergeFloat64(prevMergeState, currMergeState, currAggState, index); err != nil {
				return err
			}
		case common.TypeVarchar:
			if err := aggFunc.MergeString(prevMergeState, currMergeState, currAggState, index); err != nil {
				return err
			}
		case common.TypeTimestamp:
			if err := aggFunc.MergeTimestamp(prevMergeState, currMergeState, currAggState, index); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected column type %d", aggFunc.ValueType())
		}
	}
	return nil
}

func (a *Aggregator) loadAggregateState(keyBytes []byte, readRows *common.Rows, aggStateHolders map[string]*aggStateHolder) (*aggStateHolder, error) {
	sKey := common.ByteSliceToStringZeroCopy(keyBytes)
	stateHolder, ok := aggStateHolders[sKey] // maybe already cached for this batch
	if !ok {
		// Nope - try and load the aggregate state from storage
		rowBytes, err := a.storage.LocalGet(keyBytes)
		if err != nil {
			return nil, err
		}
		var currRow *common.Row
		if rowBytes != nil {
			// Doesn't matter if we use partial or full col types here as they are the same
			if err := common.DecodeRow(rowBytes, a.PartialAggTableInfo.ColumnTypes, readRows); err != nil {
				return nil, err
			}
			r := readRows.GetRow(readRows.RowCount() - 1)
			currRow = &r
		}
		numCols := len(a.colTypes)
		aggState := aggfuncs.NewAggState(numCols)
		stateHolder = &aggStateHolder{aggState: aggState}
		stateHolder.keyBytes = keyBytes
		aggStateHolders[sKey] = stateHolder
		if currRow != nil {
			// Initialise the agg state with the row from storage
			if err := a.initAggStateWithRow(currRow, aggState, numCols); err != nil {
				return nil, err
			}
			stateHolder.initialRow = currRow
		}

		// copy the agg state here and set it as a field on the holder
		stateHolder.initialRowBytes = rowBytes
	}
	return stateHolder, nil
}

func (a *Aggregator) storeAggregateResults(stateHolders map[string]*aggStateHolder, writeBatch *cluster.WriteBatch) error {
	resultRows := a.rowsFactory.NewRows(len(stateHolders))
	rowCount := 0
	for _, stateHolder := range stateHolders {
		aggState := stateHolder.aggState
		if aggState.IsChanged() {
			for i, colType := range a.colTypes {
				if aggState.IsNull(i) {
					resultRows.AppendNullToColumn(i)
				} else {
					switch colType.Type {
					case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
						resultRows.AppendInt64ToColumn(i, aggState.GetInt64(i))
					case common.TypeDecimal:
						resultRows.AppendDecimalToColumn(i, aggState.GetDecimal(i))
					case common.TypeDouble:
						resultRows.AppendFloat64ToColumn(i, aggState.GetFloat64(i))
					case common.TypeVarchar:
						str := aggState.GetString(i)
						resultRows.AppendStringToColumn(i, str)
					case common.TypeTimestamp:
						ts, err := aggState.GetTimestamp(i)
						if err != nil {
							return err
						}
						resultRows.AppendTimestampToColumn(i, ts)
					default:
						return fmt.Errorf("unexpected column type %d", colType)
					}
					// TODO!! store extra data
				}
			}
			row := resultRows.GetRow(rowCount)
			stateHolder.row = &row
			// Doesn't matter if we use partial or full col types here as they are the same
			valueBuff, err := common.EncodeRow(&row, a.PartialAggTableInfo.ColumnTypes, make([]byte, 0))
			if err != nil {
				return err
			}
			writeBatch.AddPut(stateHolder.keyBytes, valueBuff)
			stateHolder.rowBytes = valueBuff
			rowCount++
		}
	}
	return nil
}

func (a *Aggregator) initAggStateWithRow(currRow *common.Row, aggState *aggfuncs.AggState, numCols int) error {
	for i := 0; i < numCols; i++ {
		colType := a.colTypes[i]
		if currRow.IsNull(i) {
			aggState.SetNull(i)
		} else {
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				aggState.SetInt64(i, currRow.GetInt64(i))
			case common.TypeDecimal:
				if err := aggState.SetDecimal(i, currRow.GetDecimal(i)); err != nil {
					return err
				}
			case common.TypeDouble:
				aggState.SetFloat64(i, currRow.GetFloat64(i))
			case common.TypeVarchar:
				strVal := currRow.GetString(i)
				aggState.SetString(i, strVal)
			case common.TypeTimestamp:
				if err := aggState.SetTimestamp(i, currRow.GetTimestamp(i)); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
		}
	}
	return nil
}

func (a *Aggregator) evaluateAggFunctions(aggState *aggfuncs.AggState, row *common.Row) error {
	for index, aggFunc := range a.aggFuncs {
		switch aggFunc.ValueType().Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			arg, null, err := aggFunc.ArgExpression().EvalInt64(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalInt64(arg, null, aggState, index)
			if err != nil {
				return err
			}
		case common.TypeDecimal:
			arg, null, err := aggFunc.ArgExpression().EvalDecimal(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalDecimal(arg, null, aggState, index)
			if err != nil {
				return err
			}
		case common.TypeDouble:
			arg, null, err := aggFunc.ArgExpression().EvalFloat64(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalFloat64(arg, null, aggState, index)
			if err != nil {
				return err
			}
		case common.TypeVarchar:
			arg, null, err := aggFunc.ArgExpression().EvalString(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalString(arg, null, aggState, index)
			if err != nil {
				return err
			}
		case common.TypeTimestamp:
			arg, null, err := aggFunc.ArgExpression().EvalTimestamp(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalTimestamp(arg, null, aggState, index)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected column type %d", aggFunc.ValueType())
		}
	}
	return nil
}

func (a *Aggregator) createKey(row *common.Row, shardID uint64, colTypes []common.ColumnType, keyCols []int, tableID uint64) ([]byte, error) {
	keyBytes := table.EncodeTableKeyPrefix(tableID, shardID, 25)
	return common.EncodeKeyCols(row, keyCols, colTypes, keyBytes)
}

func createAggFunctions(aggFunctionInfos []*AggregateFunctionInfo, colTypes []common.ColumnType) ([]aggfuncs.AggregateFunction, error) {
	aggFuncs := make([]aggfuncs.AggregateFunction, len(aggFunctionInfos))
	for index, funcInfo := range aggFunctionInfos {
		argExpr := funcInfo.ArgExpr
		valueType := colTypes[index]
		aggFunc, err := aggfuncs.NewAggregateFunction(argExpr, funcInfo.FuncType, valueType)
		if err != nil {
			return nil, err
		}
		aggFuncs[index] = aggFunc
	}
	return aggFuncs, nil
}

func (a *Aggregator) ReCalcSchemaFromChildren() error {
	// NOOP
	return nil
}
