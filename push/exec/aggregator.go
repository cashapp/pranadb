package exec

import (
	"fmt"
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/squareup/pranadb/aggfuncs"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/push/util"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/table"
	"sync"
)

type Aggregator struct {
	pushExecutorBase
	aggFuncs     []aggfuncs.AggregateFunction
	AggTableInfo *common.TableInfo
	groupByCols  []int // The group by column indexes in the child
	storage      cluster.Cluster
	sharder      *sharder.Sharder
	soloAggShard int64
	keyCaches    sync.Map
	cachesLock   sync.Mutex
	lruCacheSize int
}

type AggregateFunctionInfo struct {
	FuncType   aggfuncs.AggFunctionType
	Distinct   bool
	ArgExpr    *common.Expression
	ReturnType common.ColumnType
}

type aggStateHolder struct {
	aggState   *aggfuncs.AggState
	keyBytes   []byte
	initialRow *common.Row
	row        *common.Row
}

func NewAggregator(pkCols []int, aggFunctions []*AggregateFunctionInfo, aggTableInfo *common.TableInfo,
	groupByCols []int, storage cluster.Cluster, shrdr *sharder.Sharder, lruCacheSize int) (*Aggregator, error) {

	colTypes := make([]common.ColumnType, len(aggFunctions))
	for i, aggFunc := range aggFunctions {
		colTypes[i] = aggFunc.ReturnType
	}
	aggTableInfo.ColumnTypes = colTypes
	rf := common.NewRowsFactory(colTypes)
	pushBase := pushExecutorBase{
		colTypes:    colTypes,
		keyCols:     pkCols,
		rowsFactory: rf,
	}
	aggFuncs, err := createAggFunctions(aggFunctions, colTypes)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var soloAggShard int64
	if len(groupByCols) == 0 {
		// There are no group by cols - e.g. count(*) of all rows
		// In this case we don't want to hash a nil []byte - this will work but will mean every non group by
		// aggregation would end up on the same shard - and get very hot! So we instead pre-choose the which shard is
		// going to get the aggregation by hashing the generated table name
		remoteShardID, err := shrdr.CalculateShard(sharder.ShardTypeHash,
			[]byte(fmt.Sprintf("%s.%s", aggTableInfo.SchemaName, aggTableInfo.Name)))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		soloAggShard = int64(remoteShardID)
	} else {
		soloAggShard = -1
	}
	return &Aggregator{
		pushExecutorBase: pushBase,
		aggFuncs:         aggFuncs,
		AggTableInfo:     aggTableInfo,
		groupByCols:      groupByCols,
		storage:          storage,
		sharder:          shrdr,
		soloAggShard:     soloAggShard,
		lruCacheSize:     lruCacheSize,
	}, nil
}

type stateHolders struct {
	holdersMap map[string]*aggStateHolder
}

func (a *Aggregator) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {

	// Forward the rows to the shard which owns the group by key
	numRows := rowsBatch.Len()
	for i := 0; i < numRows; i++ {
		prevRow := rowsBatch.PreviousRow(i)
		currRow := rowsBatch.CurrentRow(i)
		var row *common.Row
		if currRow != nil {
			row = currRow
		} else {
			row = prevRow
		}
		colTypes := a.GetChildren()[0].ColTypes()

		var remoteShardID uint64
		if a.soloAggShard != -1 {
			remoteShardID = uint64(a.soloAggShard)
		} else {
			keyBytes, err := common.EncodeKeyCols(row, a.groupByCols, colTypes, nil)
			if err != nil {
				return err
			}
			remoteShardID, err = a.sharder.CalculateShard(sharder.ShardTypeHash, keyBytes)
			if err != nil {
				return errors.WithStack(err)
			}
		}

		// TODO optimisation - pass through row bytes so don't have to re-encode
		var prevRowBytes []byte
		var err error
		if prevRow != nil {
			prevRowBytes, err = common.EncodeRow(prevRow, colTypes, nil)
			if err != nil {
				return err
			}
		}
		var currRowBytes []byte
		if currRow != nil {
			currRowBytes, err = common.EncodeRow(currRow, colTypes, nil)
			if err != nil {
				return err
			}
		}

		receiverSeq := rowsBatch.ReceiverIndex(i)
		if receiverSeq == 0 {
			// sanity check
			// a valid receiver sequence is never equal to zero - they always start at 1 so we know that it's
			// undefined here and we can't create a valid dedup key
			panic("undefined receiver sequence in attempting to forward aggregation")
		}
		var origTableID uint64
		if ctx.FillTableID != -1 {
			origTableID = uint64(ctx.FillTableID)
		} else {
			origTableID = a.AggTableInfo.ID
		}
		forwardKey := util.EncodeKeyForForwardAggregation(origTableID,
			ctx.WriteBatch.ShardID, uint64(receiverSeq), a.AggTableInfo.ID)
		value := util.EncodePrevAndCurrentRow(prevRowBytes, currRowBytes)
		ctx.AddToForwardBatch(remoteShardID, forwardKey, value)
	}

	return nil
}

// HandleRemoteRows is called when partial aggregation is forwarded from another shard
func (a *Aggregator) HandleRemoteRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {

	// For each shard and aggregator we maintain a small cache that holds recently accessed rows from the aggregation
	// this helps in the case the aggregation has a reasonably small number of rows (a common case) as it means we
	// don't have to load the current row every time from storage before we recalculate the aggregation
	// This cache is different from the holders map which caches every aggregate row but is scoped to the current batch
	// The lru cache is not guaranteed to hold every row in the batch
	keyCache, err := a.getCacheForShard(ctx.WriteBatch.ShardID)
	if err != nil {
		return err
	}

	// Calculate the aggregations
	holders := &stateHolders{holdersMap: make(map[string]*aggStateHolder)}
	numRows := rowsBatch.Len()
	readRows := a.rowsFactory.NewRows(numRows)
	for i := 0; i < numRows; i++ {
		prevRow := rowsBatch.PreviousRow(i)
		currentRow := rowsBatch.CurrentRow(i)
		if err := a.calcAggregations(ctx, prevRow, currentRow, readRows, holders, ctx.WriteBatch.ShardID, keyCache); err != nil {
			return err
		}
	}

	// Store the results locally
	if err := a.storeAggregateResults(holders, ctx.WriteBatch); err != nil {
		return errors.WithStack(err)
	}

	resultRows := a.rowsFactory.NewRows(numRows)
	entries := make([]RowsEntry, 0, numRows)
	rc := 0

	// Send the rows to the parent
	for _, stateHolder := range holders.holdersMap {
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
			entries = append(entries, NewRowsEntry(pi, ci, -1))
			// And put it in the lru cache
			sKey := common.ByteSliceToStringZeroCopy(stateHolder.keyBytes)
			keyCache.Add(sKey, stateHolder)
		}
	}

	return a.parent.HandleRows(NewRowsBatch(resultRows, entries), ctx)
}

func (a *Aggregator) calcAggregations(ctx *ExecutionContext, prevRow *common.Row, currRow *common.Row, readRows *common.Rows,
	aggStateHolders *stateHolders, shardID uint64, keyCache *simplelru.LRU) error {

	// Create the key
	keyBytes, err := a.createKeyFromPrevOrCurrRow(prevRow, currRow, shardID, a.GetChildren()[0].ColTypes(), a.groupByCols, a.AggTableInfo.ID)
	if err != nil {
		return errors.WithStack(err)
	}

	// Lookup existing aggregate state
	stateHolder, err := a.loadAggregateState(ctx, keyBytes, readRows, aggStateHolders, keyCache)
	if err != nil {
		return errors.WithStack(err)
	}

	// Evaluate the agg functions on the state
	if prevRow != nil {
		if err := a.evaluateAggFunctions(stateHolder.aggState, prevRow, true); err != nil {
			return err
		}
	}
	if currRow != nil {
		if err := a.evaluateAggFunctions(stateHolder.aggState, currRow, false); err != nil {
			return err
		}
	}
	return nil
}

func (a *Aggregator) loadAggregateState(ctx *ExecutionContext, keyBytes []byte, readRows *common.Rows, aggStateHolders *stateHolders,
	keyCache *simplelru.LRU) (*aggStateHolder, error) {
	sKey := common.ByteSliceToStringZeroCopy(keyBytes)
	stateHolder, ok := aggStateHolders.holdersMap[sKey] // maybe already cached for this batch
	if !ok {
		// Look in the LRU cache
		v, ok := keyCache.Get(sKey)
		if ok {
			stateHolder, ok = v.(*aggStateHolder)
			if !ok {
				panic("not an *aggStateHolder")
			}
			aggStateHolders.holdersMap[sKey] = stateHolder
			stateHolder.aggState.SetUnchanged()
			stateHolder.initialRow = stateHolder.row
			stateHolder.row = nil
		} else {
			// Try and load the aggregate state from storage
			rowBytes, err := ctx.Getter.Get(keyBytes)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			var currRow *common.Row
			if rowBytes != nil {
				// Doesn't matter if we use partial or full col types here as they are the same
				if err := common.DecodeRow(rowBytes, a.AggTableInfo.ColumnTypes, readRows); err != nil {
					return nil, errors.WithStack(err)
				}
				r := readRows.GetRow(readRows.RowCount() - 1)
				currRow = &r
			}
			numCols := len(a.colTypes)
			aggState := aggfuncs.NewAggState(numCols)
			stateHolder = &aggStateHolder{
				aggState: aggState,
			}
			stateHolder.keyBytes = keyBytes
			aggStateHolders.holdersMap[sKey] = stateHolder
			if currRow != nil {
				// Initialise the agg state with the row from storage
				if err := a.initAggStateWithRow(currRow, aggState, numCols); err != nil {
					return nil, errors.WithStack(err)
				}
				stateHolder.initialRow = currRow
			}

			// And put in lru cache
			keyCache.Add(sKey, stateHolder)
		}
	}
	return stateHolder, nil
}

func (a *Aggregator) storeAggregateResults(stateHolders *stateHolders, writeBatch *cluster.WriteBatch) error {
	resultRows := a.rowsFactory.NewRows(len(stateHolders.holdersMap))
	rowCount := 0
	for _, stateHolder := range stateHolders.holdersMap {
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
							return errors.WithStack(err)
						}
						resultRows.AppendTimestampToColumn(i, ts)
					default:
						return errors.Errorf("unexpected column type %d", colType)
					}
					// TODO!! store extra data
				}
			}
			row := resultRows.GetRow(rowCount)
			stateHolder.row = &row
			// Doesn't matter if we use partial or full col types here as they are the same
			valueBuff, err := common.EncodeRow(&row, a.AggTableInfo.ColumnTypes, make([]byte, 0))
			if err != nil {
				return errors.WithStack(err)
			}
			writeBatch.AddPut(stateHolder.keyBytes, valueBuff)
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
					return errors.WithStack(err)
				}
			case common.TypeDouble:
				aggState.SetFloat64(i, currRow.GetFloat64(i))
			case common.TypeVarchar:
				strVal := currRow.GetString(i)
				aggState.SetString(i, strVal)
			case common.TypeTimestamp:
				if err := aggState.SetTimestamp(i, currRow.GetTimestamp(i)); err != nil {
					return errors.WithStack(err)
				}
			default:
				return errors.Errorf("unexpected column type %d", colType)
			}
		}
	}
	return nil
}

func (a *Aggregator) evaluateAggFunctions(aggState *aggfuncs.AggState, row *common.Row, reverse bool) error {
	for index, aggFunc := range a.aggFuncs {
		switch aggFunc.ValueType().Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			arg, null, err := aggFunc.ArgExpression().EvalInt64(row)
			if err != nil {
				return errors.WithStack(err)
			}
			err = aggFunc.EvalInt64(arg, null, aggState, index, reverse)
			if err != nil {
				return errors.WithStack(err)
			}
		case common.TypeDecimal:
			arg, null, err := aggFunc.ArgExpression().EvalDecimal(row)
			if err != nil {
				return errors.WithStack(err)
			}
			err = aggFunc.EvalDecimal(arg, null, aggState, index, reverse)
			if err != nil {
				return errors.WithStack(err)
			}
		case common.TypeDouble:
			arg, null, err := aggFunc.ArgExpression().EvalFloat64(row)
			if err != nil {
				return errors.WithStack(err)
			}
			err = aggFunc.EvalFloat64(arg, null, aggState, index, reverse)
			if err != nil {
				return errors.WithStack(err)
			}
		case common.TypeVarchar:
			arg, null, err := aggFunc.ArgExpression().EvalString(row)
			if err != nil {
				return errors.WithStack(err)
			}
			err = aggFunc.EvalString(arg, null, aggState, index, reverse)
			if err != nil {
				return errors.WithStack(err)
			}
		case common.TypeTimestamp:
			arg, null, err := aggFunc.ArgExpression().EvalTimestamp(row)
			if err != nil {
				return errors.WithStack(err)
			}
			err = aggFunc.EvalTimestamp(arg, null, aggState, index, reverse)
			if err != nil {
				return errors.WithStack(err)
			}
		default:
			return errors.Errorf("unexpected column type %d", aggFunc.ValueType())
		}
	}
	return nil
}

func (a *Aggregator) createKeyFromPrevOrCurrRow(prevRow *common.Row, currRow *common.Row, shardID uint64, colTypes []common.ColumnType, keyCols []int, tableID uint64) ([]byte, error) {
	keyBytes := table.EncodeTableKeyPrefix(tableID, shardID, 25)
	var row *common.Row
	if currRow != nil {
		row = currRow
	} else {
		row = prevRow
	}
	return common.EncodeKeyCols(row, keyCols, colTypes, keyBytes)
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
			return nil, errors.WithStack(err)
		}
		aggFuncs[index] = aggFunc
	}
	return aggFuncs, nil
}

func (a *Aggregator) ReCalcSchemaFromChildren() error {
	// NOOP
	return nil
}

func (a *Aggregator) getCacheForShard(shardID uint64) (*simplelru.LRU, error) {
	var cache *simplelru.LRU
	v, ok := a.keyCaches.Load(shardID)
	if !ok {
		a.cachesLock.Lock()
		defer a.cachesLock.Unlock()
		v, ok = a.keyCaches.Load(shardID)
		if !ok {
			var err error
			cache, err = simplelru.NewLRU(a.lruCacheSize, nil)
			if err != nil {
				return nil, err
			}
			a.keyCaches.Store(shardID, cache)
		}
	}
	if cache == nil {
		cache, ok = v.(*simplelru.LRU)
		if !ok {
			panic("not a *simplelru.LRU")
		}
	}
	return cache, nil
}

func (a *Aggregator) ShardFailed(shardID uint64) {
	// We clear the cache for the shard as the data could be retried
	a.keyCaches.Delete(shardID)
}
