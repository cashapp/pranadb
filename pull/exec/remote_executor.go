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
	query          string
	cluster        cluster.Cluster
	RemoteDag      PullExecutor
	completeCount  int
}

func NewRemoteExecutor(remoteDag PullExecutor, colTypes []common.ColumnType, schemaName string, query string, queryID string, cluster cluster.Cluster) *RemoteExecutor {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	pg := RemoteExecutor{
		pullExecutorBase: base,
		schemaName:       schemaName,
		query:            query,
		cluster:          cluster,
		RemoteDag:        remoteDag,
	}
	shardIDs := cluster.GetAllShardIDs()
	pg.clusterGetters = createGetters(queryID, shardIDs, &pg)
	return &pg
}

func createGetters(queryIDBase string, shardIDs []uint64, gatherer *RemoteExecutor) []*clusterGetter {
	getters := make([]*clusterGetter, len(shardIDs))
	for i, shardID := range shardIDs {
		getters[i] = &clusterGetter{
			shardID:  shardID,
			gatherer: gatherer,
			// We append the shardID to the query ID as, at the remote end, we need a unique query DAG for each
			// shard
			queryID: fmt.Sprintf("%s-%d", queryIDBase, shardID),
		}
		getters[i].complete.Store(false)
	}
	return getters
}

type clusterGetter struct {
	shardID  uint64
	gatherer *RemoteExecutor
	queryID  string
	complete atomic.Value
}

func (c *clusterGetter) GetRows(limit int) (resultChan chan cluster.RemoteQueryResult) {
	ch := make(chan cluster.RemoteQueryResult)
	go func() {
		rows, err := c.gatherer.cluster.ExecuteRemotePullQuery(c.gatherer.schemaName, c.gatherer.query, c.queryID, limit, c.shardID, c.gatherer.rowsFactory)
		isComplete := rows.RowCount() < limit
		c.complete.Store(isComplete)
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

func (p *RemoteExecutor) GetRows(limit int) (rows *common.Rows, err error) {

	if limit == 0 || limit < -1 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}

	numGetters := len(p.clusterGetters)
	channels := make([]chan cluster.RemoteQueryResult, numGetters)
	if limit == -1 {
		// We can't have an unlimited limit as we're shifting over the network
		limit = 1000
	}
	rows = p.rowsFactory.NewRows(limit)

	if p.completeCount == numGetters {
		return rows, nil
	}

	// TODO this algorithm can be improved
	// We execute these in parallel
	for p.completeCount < numGetters {

		totToGet := limit - rows.RowCount()
		toGet := totToGet / (numGetters - p.completeCount)
		if toGet == 0 {
			toGet = 1
		}
		var gettersCalled []int
		getsRequested := 0
		for i, getter := range p.clusterGetters {
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
			getter := p.clusterGetters[i]

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
				p.completeCount++
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
