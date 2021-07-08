package exec

import (
	"errors"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
)

type RemoteExecutor struct {
	pullExecutorBase
	clusterGetters []*clusterGetter
	schemaName     string
	query          string
	queryID        string
	cluster        cluster.Cluster
	RemoteDag      PullExecutor
}

func NewRemoteExecutor(colTypes []common.ColumnType, remoteDag PullExecutor, schemaName string, query string, queryID string, nodeIDs []int, cluster cluster.Cluster) *RemoteExecutor {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	pg := RemoteExecutor{
		pullExecutorBase: base,
		schemaName:       schemaName,
		query:            query,
		queryID:          queryID,
		cluster:          cluster,
		RemoteDag:        remoteDag,
	}
	pg.clusterGetters = createGetters(nodeIDs, &pg)
	return &pg
}

func createGetters(nodeIDs []int, gatherer *RemoteExecutor) []*clusterGetter {
	getters := make([]*clusterGetter, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		getters[i] = &clusterGetter{
			nodeID:   nodeID,
			gatherer: gatherer,
		}
	}
	return getters
}

type clusterGetter struct {
	nodeID   int
	gatherer *RemoteExecutor
}

func (c *clusterGetter) GetRows(limit int) (resultChan chan cluster.RemoteQueryResult) {
	return c.gatherer.cluster.ExecuteRemotePullQuery(c.gatherer.schemaName, c.gatherer.query, c.gatherer.queryID, limit, c.nodeID)
}

func (p *RemoteExecutor) GetRows(limit int) (rows *common.Rows, err error) {

	// TODO this can be optimised

	numGetters := len(p.clusterGetters)
	channels := make([]chan cluster.RemoteQueryResult, numGetters)
	complete := make([]bool, numGetters)
	rows = p.rowsFactory.NewRows(limit)

	completeCount := 0

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
