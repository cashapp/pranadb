package exec

import (
	"errors"
	"fmt"

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
}

func NewRemoteExecutor(colTypes []common.ColumnType, remoteDag PullExecutor, schemaName string, query string, queryID string, cluster cluster.Cluster) *RemoteExecutor {
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
	}
	return getters
}

type clusterGetter struct {
	shardID  uint64
	gatherer *RemoteExecutor
	queryID  string
}

func (c *clusterGetter) GetRows(limit int) (resultChan chan cluster.RemoteQueryResult) {
	ch := make(chan cluster.RemoteQueryResult)
	go func() {
		rows, err := c.gatherer.cluster.ExecuteRemotePullQuery(c.gatherer.schemaName, c.gatherer.query, c.queryID, limit, c.shardID, c.gatherer.rowsFactory)
		ch <- cluster.RemoteQueryResult{
			Rows: rows,
			Err:  err,
		}
	}()
	return ch
}

func (p *RemoteExecutor) GetRows(limit int) (rows *common.Rows, err error) {

	// TODO this can be optimised

	numGetters := len(p.clusterGetters)
	channels := make([]chan cluster.RemoteQueryResult, numGetters)
	complete := make([]bool, numGetters)
	rows = p.rowsFactory.NewRows(limit)

	completeCount := 0

	// We execute these in parallel
	for completeCount < numGetters {
		toGet := (limit - rows.RowCount()) / (numGetters - completeCount)
		for i, getter := range p.clusterGetters {
			if !complete[i] {
				channels[i] = getter.GetRows(toGet)
			}
		}
		for i, ch := range channels {
			if !complete[i] {
				res, ok := <-ch
				if !ok {
					return nil, errors.New("channel was closed")
				}
				if res.Err != nil {
					return nil, res.Err
				}
				if res.Rows.RowCount() < toGet {
					complete[i] = true
					completeCount++
				}
				for i := 0; i < res.Rows.RowCount(); i++ {
					rows.AppendRow(res.Rows.GetRow(i))
				}
			}
		}
	}

	return rows, nil
}
