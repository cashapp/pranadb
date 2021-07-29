package exec

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
)

type RemoteExecutor struct {
	pullExecutorBase
	clusterGetters []*clusterGetter
	schemaName     string
	cluster        cluster.Cluster
	completeCount  int
	queryInfo      *cluster.QueryExecutionInfo
	RemoteDag      PullExecutor
	ShardIDs       []uint64
}

func NewRemoteExecutor(remoteDAG PullExecutor, queryInfo *cluster.QueryExecutionInfo, colTypes []common.ColumnType, schemaName string, cluster cluster.Cluster) *RemoteExecutor {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	re := RemoteExecutor{
		pullExecutorBase: base,
		schemaName:       schemaName,
		cluster:          cluster,
		queryInfo:        queryInfo,
		RemoteDag:        remoteDAG,
		ShardIDs:         cluster.GetAllShardIDs(),
	}
	re.createGetters()
	return &re
}

type clusterGetter struct {
	shardID       uint64
	re            *RemoteExecutor
	complete      atomic.Value
	queryExecInfo cluster.QueryExecutionInfo
}

func (c *clusterGetter) GetRows(limit int) (resultChan chan cluster.RemoteQueryResult) {
	ch := make(chan cluster.RemoteQueryResult)
	go func() {
		var rows *common.Rows
		var err error
		c.queryExecInfo.Limit = uint32(limit)
		rows, err = c.re.cluster.ExecuteRemotePullQuery(&c.queryExecInfo, c.re.rowsFactory)
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

func (re *RemoteExecutor) Reset() {
	// It's a prepared statement and it's being reused
	// Sanity check
	if !re.queryInfo.IsPs {
		panic("only prepared statements can be reused")
	}
	re.createGetters()
	re.completeCount = 0
	re.pullExecutorBase.Reset()
}

func (re *RemoteExecutor) GetRows(limit int) (rows *common.Rows, err error) {

	if limit == 0 || limit < -1 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}

	numGetters := len(re.clusterGetters)
	channels := make([]chan cluster.RemoteQueryResult, numGetters)
	if limit == -1 {
		// We can't have an unlimited limit as we're shifting over the network
		limit = 1000
	}
	rows = re.rowsFactory.NewRows(limit)

	// TODO this algorithm can be improved
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
				return nil, errors.New("channel was closed")
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

func (re *RemoteExecutor) createGetters() {
	re.clusterGetters = make([]*clusterGetter, len(re.ShardIDs))
	for i, shardID := range re.ShardIDs {
		// Make a copy of the query exec info as they need different shard ids, session id and limit (later)
		qei := *re.queryInfo
		// We append the shard id to the session id - as on each remote node we need to maintain a different
		// session for each shard, as each shard wil have an independent current query running and needs an
		// independent planner as they're not thread-safe
		qei.SessionID = fmt.Sprintf("%s-%d", re.queryInfo.SessionID, shardID)
		qei.ShardID = shardID
		re.clusterGetters[i] = &clusterGetter{
			shardID:       shardID,
			re:            re,
			queryExecInfo: qei,
		}
		re.clusterGetters[i].complete.Store(false)
	}
}
