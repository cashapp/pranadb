package exec

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/meta"
	"strings"
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

func NewRemoteExecutor(remoteDAG PullExecutor, queryInfo *cluster.QueryExecutionInfo, colNames []string, colTypes []common.ColumnType, schemaName string, cluster cluster.Cluster) *RemoteExecutor {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colNames:       colNames,
		colTypes:       colTypes,
		simpleColNames: common.ToSimpleColNames(colNames),
		rowsFactory:    rf,
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

func (p *RemoteExecutor) GetRows(limit int) (rows *common.Rows, err error) {
	rows, err = p.getRows(limit)
	if err != nil {
		log.Printf("Failed to get rows in remoteexecutor %+v", err)
	}
	return rows, err
}

func (re *RemoteExecutor) getRows(limit int) (rows *common.Rows, err error) {

	if limit < 1 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}

	numGetters := len(re.clusterGetters)
	channels := make([]chan cluster.RemoteQueryResult, numGetters)

	rows = re.rowsFactory.NewRows(100)

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
	shardIDs := re.ShardIDs
	if re.schemaName == meta.SystemSchemaName {
		// It's only the tables sys table that doesn't require fanout, others do, e.g. offsets
		if strings.Index(strings.ToLower(re.queryInfo.Query), fmt.Sprintf("from %s ", meta.TableDefTableName)) != -1 {
			shardIDs = []uint64{cluster.SystemSchemaShardID}
		}
	}
	re.clusterGetters = make([]*clusterGetter, len(shardIDs))
	for i, shardID := range shardIDs {
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
