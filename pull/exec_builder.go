package pull

import (
	"strings"

	"github.com/pingcap/parser/model"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/sess"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/tidb/planner"
	"github.com/squareup/pranadb/tidb/planner/util"
	"github.com/squareup/pranadb/tidb/util/ranger"
)

func (p *Engine) buildPullDAGWithSort(session *sess.Session, logicalPlan planner.LogicalPlan,
	physicalPlan planner.PhysicalPlan, remote bool) (exec.PullExecutor, error) {
	// Build dag from the plan
	dag, err := p.buildPullDAG(session, physicalPlan, remote)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	logicalSort, ok := logicalPlan.(*planner.LogicalSort)
	if ok {
		_, hasPhysicalSort := dag.(*exec.PullSort)
		if !hasPhysicalSort {
			// The TiDB planner assumes range partitioning and therefore sometimes elides the physical sort operator
			// from the physical plan during the optimisation process if the order by is on the primary key of the table
			// In this case it thinks the table is already ordered and we fan out to remote nodes using range scans
			// so the iteration order of the partial results when joined together gives us ordered results as require.
			// However, we use hash partitioning, so we always need to implement the sort after all partial results
			// are returned from nodes.
			// So... if the logical plan has a sort, but the physical plan doesn't we need to add a sort executor
			// in manually, which is what we're doing here

			desc, sortByExprs := p.byItemsToDescAndSortExpression(logicalSort.ByItems)
			sortExec := exec.NewPullSort(dag.ColNames(), dag.ColTypes(), desc, sortByExprs)
			exec.ConnectPullExecutors([]exec.PullExecutor{dag}, sortExec)
			dag = sortExec
		}
	}

	return dag, nil
}

// TODO: extract functions and break apart giant switch
// nolint: gocyclo
func (p *Engine) buildPullDAG(session *sess.Session, plan planner.PhysicalPlan, remote bool) (exec.PullExecutor, error) {
	cols := plan.Schema().Columns
	colTypes := make([]common.ColumnType, 0, len(cols))
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		colType := col.GetType()
		pranaType := common.ConvertTiDBTypeToPranaType(colType)
		colTypes = append(colTypes, pranaType)
		colNames = append(colNames, col.OrigName)
	}
	var executor exec.PullExecutor
	var err error
	switch op := plan.(type) {
	case *planner.PhysicalProjection:
		var exprs []*common.Expression
		for _, expr := range op.Exprs {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor, err = exec.NewPullProjection(colNames, colTypes, exprs)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case *planner.PhysicalSelection:
		var exprs []*common.Expression
		for _, expr := range op.Conditions {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor = exec.NewPullSelect(colNames, colTypes, exprs)
		if err != nil {
			return nil, errors.WithStack(err)
		}

	case *planner.PhysicalTableScan:
		if remote {
			tableName := op.Table.Name.L
			executor, err = p.createPullTableScan(session.Schema, tableName, op.Ranges, op.Columns, session.QueryInfo.ShardID)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		} else {
			remoteDag, err := p.buildPullDAG(session, op, true)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			pointGetShardID, err := p.getPointGetShardID(session, op.Ranges, op.Table.Name.L)
			if err != nil {
				return nil, err
			}
			executor = exec.NewRemoteExecutor(remoteDag, session.QueryInfo, colNames, colTypes, session.Schema.Name, p.cluster,
				pointGetShardID)
		}
	case *planner.PhysicalIndexScan:
		if remote {
			tableName := op.Table.Name.L
			indexName := op.Index.Name.L
			executor, err = p.createPullIndexScan(session.Schema, tableName, indexName, op.Ranges, op.Columns, session.QueryInfo.ShardID)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		} else {
			remoteDag, err := p.buildPullDAG(session, op, true)
			if err != nil {
				return nil, err
			}
			executor = exec.NewRemoteExecutor(remoteDag, session.QueryInfo, remoteDag.ColNames(), remoteDag.ColTypes(), session.Schema.Name, p.cluster,
				-1)
		}
	case *planner.PhysicalSort:
		desc, sortByExprs := p.byItemsToDescAndSortExpression(op.ByItems)
		executor = exec.NewPullSort(colNames, colTypes, desc, sortByExprs)
	default:
		return nil, errors.Errorf("unexpected plan type %T", plan)
	}

	var childExecutors []exec.PullExecutor
	for _, child := range plan.Children() {
		childExecutor, err := p.buildPullDAG(session, child, remote)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if childExecutor != nil {
			childExecutors = append(childExecutors, childExecutor)
		}
	}
	exec.ConnectPullExecutors(childExecutors, executor)

	return executor, nil
}

func (p *Engine) getPointGetShardID(session *sess.Session, ranges []*ranger.Range, tableName string) (int64, error) {
	var pointGetShardID int64 = -1
	if len(ranges) == 1 {
		rng := ranges[0]
		if rng.IsPoint(session.Planner().StatementContext()) {
			if len(rng.LowVal) != 1 {
				// Composite ranges not supported yet
				return -1, nil
			}
			table, ok := session.Schema.GetTable(tableName)
			if !ok {
				return 0, errors.Errorf("cannot find table %s", tableName)
			}
			if table.GetTableInfo().ColumnTypes[table.GetTableInfo().PrimaryKeyCols[0]].Type == common.TypeDecimal {
				// We don't currently support optimised point gets for keys of type Decimal
				return -1, nil
			}
			v := common.TiDBValueToPranaValue(rng.LowVal[0].GetValue())
			k := []interface{}{v}

			key, err := common.EncodeKey(k, table.GetTableInfo().ColumnTypes, table.GetTableInfo().PrimaryKeyCols, []byte{})
			if err != nil {
				return 0, err
			}
			pgsid, err := p.shrder.CalculateShard(sharder.ShardTypeHash, key)
			if err != nil {
				return 0, err
			}
			pointGetShardID = int64(pgsid)
		}
	}
	return pointGetShardID, nil
}

func (p *Engine) createPullTableScan(schema *common.Schema, tableName string, ranges []*ranger.Range, columns []*model.ColumnInfo, shardID uint64) (exec.PullExecutor, error) {
	tbl, ok := schema.GetTable(tableName)
	if !ok {
		return nil, errors.Errorf("unknown source or materialized view %s", tableName)
	}
	if len(ranges) > 1 {
		return nil, errors.Error("only one range supported")
	}
	var scanRange *exec.ScanRange
	if len(ranges) == 1 {
		rng := ranges[0]
		if !rng.IsFullRange() {
			if len(rng.LowVal) != 1 {
				return nil, errors.Error("composite ranges not supported")
			}
			lowD := rng.LowVal[0]
			highD := rng.HighVal[0]
			scanRange = &exec.ScanRange{
				LowVal:   common.TiDBValueToPranaValue(lowD.GetValue()),
				HighVal:  common.TiDBValueToPranaValue(highD.GetValue()),
				LowExcl:  rng.LowExclude,
				HighExcl: rng.HighExclude,
			}
		}
	}
	var colIndexes []int
	for _, col := range columns {
		colIndexes = append(colIndexes, col.Offset)
	}
	return exec.NewPullTableScan(tbl.GetTableInfo(), colIndexes, p.cluster, shardID, scanRange)
}

func (p *Engine) createPullIndexScan(schema *common.Schema, tableName string, indexName string, ranges []*ranger.Range, columns []*model.ColumnInfo, shardID uint64) (exec.PullExecutor, error) {
	tbl, ok := schema.GetTable(tableName)
	if !ok {
		return nil, errors.Errorf("unknown source or materialized view %s", tableName)
	}
	idx, ok := tbl.GetTableInfo().IndexInfos[strings.Replace(indexName, tableName+"_", "", 1)]
	if !ok {
		return nil, errors.Errorf("unknown index %s", indexName)
	}
	var scanRanges []*exec.ScanRange
	for _, rng := range ranges {
		if !rng.IsFullRange() {
			if len(rng.LowVal) != 1 {
				return nil, errors.Error("composite ranges not supported")
			}
			lowD := rng.LowVal[0]
			highD := rng.HighVal[0]
			scanRanges = append(scanRanges, &exec.ScanRange{
				LowVal:   common.TiDBValueToPranaValue(lowD.GetValue()),
				HighVal:  common.TiDBValueToPranaValue(highD.GetValue()),
				LowExcl:  rng.LowExclude,
				HighExcl: rng.HighExclude,
			})
		}
	}
	var colIndexes []int
	for _, col := range columns {
		colIndexes = append(colIndexes, col.Offset)
	}
	return exec.NewPullIndexReader(tbl.GetTableInfo(), idx, colIndexes, p.cluster, shardID, scanRanges)
}

func (p *Engine) byItemsToDescAndSortExpression(byItems []*util.ByItems) ([]bool, []*common.Expression) {
	lbi := len(byItems)
	desc := make([]bool, lbi)
	sortByExprs := make([]*common.Expression, lbi)
	for i, byitem := range byItems {
		desc[i] = byitem.Desc
		sortByExprs[i] = common.NewExpression(byitem.Expr)
	}
	return desc, sortByExprs
}
