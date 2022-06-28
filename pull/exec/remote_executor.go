package exec

import (
	"fmt"
	"github.com/squareup/pranadb/meta"
	"strings"
	"sync/atomic"

	"github.com/squareup/pranadb/errors"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
)

type RemoteExecutor struct {
	pullExecutorBase
	clusterGetters          []*clusterGetter
	schemaName              string
	cluster                 cluster.Cluster
	completeCount           int
	queryInfo               *cluster.QueryExecutionInfo
	RemoteDag               PullExecutor
	ShardIDs                []uint64
	singlePointGetQueryInfo *cluster.QueryExecutionInfo
}

func NewRemoteExecutor(remoteDAG PullExecutor, queryInfo *cluster.QueryExecutionInfo, colNames []string,
	colTypes []common.ColumnType, schemaName string, clust cluster.Cluster, pointGetShardIDs []uint64) *RemoteExecutor {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	re := RemoteExecutor{
		pullExecutorBase: base,
		schemaName:       schemaName,
		cluster:          clust,
		queryInfo:        queryInfo,
		RemoteDag:        remoteDAG,
	}

	// The tables table is a special case and always gets stored in a single shard cluster.SystemSchemaShardID
	// We do this because we need to guarantee deterministic updates across the cluster for all of tables table
	if re.schemaName == meta.SystemSchemaName {
		re.queryInfo.SystemQuery = true
		lq := strings.ToLower(re.queryInfo.Query)
		if (strings.Index(lq, fmt.Sprintf("from %s ", meta.TableDefTableName)) != -1) ||
			(strings.Index(lq, fmt.Sprintf("from %s ", meta.IndexDefTableName)) != -1) {
			re.singlePointGetQueryInfo = re.createGetterQueryExecInfo(re.queryInfo, cluster.SystemSchemaShardID)
			return &re
		}
	}
	numPointGets := len(pointGetShardIDs)
	if numPointGets == 1 {
		// It's a single point get
		re.singlePointGetQueryInfo = re.createGetterQueryExecInfo(re.queryInfo, pointGetShardIDs[0])
	} else if numPointGets > 1 {
		// Multiple point get
		re.ShardIDs = pointGetShardIDs
		re.createGetters()
	} else {
		// Not a point get
		re.ShardIDs = clust.GetAllShardIDs()
		re.createGetters()
	}
	return &re
}

type clusterGetter struct {
	shardID       uint64
	re            *RemoteExecutor
	complete      atomic.Value
	queryExecInfo *cluster.QueryExecutionInfo
}

func (c *clusterGetter) GetRows(limit int) (resultChan chan cluster.RemoteQueryResult) {
	ch := make(chan cluster.RemoteQueryResult)
	go func() {
		var rows *common.Rows
		var err error
		c.queryExecInfo.Limit = uint32(limit)
		rows, err = c.re.cluster.ExecuteRemotePullQuery(c.queryExecInfo, c.re.rowsFactory)
		if err == nil {
			c.complete.Store(rows.RowCount() < limit)
		}
		ch <- cluster.RemoteQueryResult{
			Rows: rows,
			Err:  err,
		}
	}()
	return ch
}

func (c *clusterGetter) isComplete() bool {
	complete, ok := c.complete.Load().(bool)
	if !ok {
		panic("not a bool")
	}
	return complete
}

func (re *RemoteExecutor) GetRows(limit int) (rows *common.Rows, err error) {
	if limit < 1 {
		return nil, errors.Errorf("invalid limit %d", limit)
	}

	if re.singlePointGetQueryInfo != nil {
		// It's a single point get so we only talk to one shard - we optimise this special case by calling directly
		// and not running on different goroutines
		re.singlePointGetQueryInfo.Limit = uint32(limit)
		return re.cluster.ExecuteRemotePullQuery(re.singlePointGetQueryInfo, re.rowsFactory)
	}

	numGetters := len(re.clusterGetters)
	channels := make([]chan cluster.RemoteQueryResult, numGetters)

	rows = re.rowsFactory.NewRows(100)

	// We execute these in parallel
	for re.completeCount < numGetters {

		totToGet := limit - rows.RowCount()
		toGet := totToGet / (numGetters - re.completeCount)
		if toGet == 0 {
			toGet = 1
		}
		var gettersCalled []int
		getsRequested := 0
		for i, getter := range re.clusterGetters {
			if !getter.isComplete() {
				channels[i] = getter.GetRows(toGet)
				getsRequested += toGet
				gettersCalled = append(gettersCalled, i)
				if getsRequested == totToGet {
					break
				}
			}
		}

		for _, i := range gettersCalled {
			ch := channels[i]
			getter := re.clusterGetters[i]

			res, ok := <-ch
			if !ok {
				return nil, errors.Error("channel was closed")
			}
			if res.Err != nil {
				return nil, res.Err
			}
			if res.Rows.RowCount() > toGet {
				panic("returned too many rows")
			}
			rows.AppendAll(res.Rows)
			if getter.isComplete() {
				re.completeCount++
			}
		}
		if rows.RowCount() > limit {
			panic("too many total rows")
		}
		if rows.RowCount() == limit {
			break
		}
	}

	return rows, nil
}

// Close - We override the Close method for PullExecutor and we call our remote shards with limit zero.
// This signals them to release any resources.
// Close is only called when the query is closed before all rows are returned - e.g. if a limit has been provided on
// the query and has been reached
func (re *RemoteExecutor) Close() {
	for _, getter := range re.clusterGetters {
		if !getter.isComplete() {
			getter.GetRows(0)
		}
	}
}

func (re *RemoteExecutor) createGetters() {
	re.clusterGetters = make([]*clusterGetter, len(re.ShardIDs))
	for i, shardID := range re.ShardIDs {
		qei := re.createGetterQueryExecInfo(re.queryInfo, shardID)
		cg := &clusterGetter{
			shardID:       shardID,
			re:            re,
			queryExecInfo: qei,
		}
		cg.complete.Store(false)
		re.clusterGetters[i] = cg
	}
}

func (re *RemoteExecutor) createGetterQueryExecInfo(qei *cluster.QueryExecutionInfo, shardID uint64) *cluster.QueryExecutionInfo {
	// Note we create a copy of the query info as each instance has its own shard id etc
	qeiCopy := *qei

	// We append the shard id to the session id - as on each remote node we need to maintain a different
	// session for each shard, as each shard wil have an independent current query running and needs an
	// independent planner as they're not thread-safe
	qeiCopy.ExecutionID = fmt.Sprintf("%s-%d", re.queryInfo.ExecutionID, shardID)
	qeiCopy.ShardID = shardID
	return &qeiCopy
}
