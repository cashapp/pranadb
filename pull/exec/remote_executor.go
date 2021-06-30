package exec

import (
	"errors"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
)

type RemoteExecutor struct {
	pullExecutorBase
	asyncRowGetters []*clusterGetter
	queryID         string
	serializedDag   []byte
	cluster         cluster.Cluster
}

func NewRemoteExecutor(colTypes []common.ColumnType, queryID string, remoteDag PullExecutor, nodeIDs []int, cluster cluster.Cluster) (*RemoteExecutor, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pullExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	pg := RemoteExecutor{
		pullExecutorBase: base,
		queryID:          queryID,
		serializedDag:    SerializeDAG(remoteDag),
		cluster:          cluster,
	}
	pg.asyncRowGetters = createGetters(nodeIDs, &pg)
	return &pg, nil
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

func (c clusterGetter) GetRows(limit int, queryID string) (resultChan chan cluster.AsyncRowGetterResult) {
	return c.gatherer.cluster.ExecuteRemotePullQuery(c.gatherer.serializedDag, queryID, limit, c.nodeID)
}

func (p *RemoteExecutor) GetRows(limit int) (rows *common.Rows, err error) {

	// TODO this can be optimised

	numGetters := len(p.asyncRowGetters)
	channels := make([]chan cluster.AsyncRowGetterResult, numGetters)
	complete := make([]bool, numGetters)
	rows = p.rowsFactory.NewRows(limit)

	completeCount := 0

	for completeCount < numGetters {
		toGet := (limit - rows.RowCount()) / (numGetters - completeCount)
		for i, getter := range p.asyncRowGetters {
			if !complete[i] {
				channels[i] = getter.GetRows(toGet, p.queryID)
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

func SerializeDAG(dag PullExecutor) []byte {
	// TODO
	return nil
}

func DeserializeDAG(buf []byte) (PullExecutor, error) {
	// TODO
	return nil, nil
}
