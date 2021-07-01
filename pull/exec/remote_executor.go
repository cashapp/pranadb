package exec

import (
	"errors"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"unsafe"
)

type RemoteExecutor struct {
	pullExecutorBase
	clusterGetters []*clusterGetter
	queryID        string
	serializedDag  []byte
	cluster        cluster.Cluster
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
	serialized, err := SerializeDAG(remoteDag, make([]byte, 32))
	pg := RemoteExecutor{
		pullExecutorBase: base,
		queryID:          queryID,
		serializedDag:    serialized,
		cluster:          cluster,
	}
	pg.clusterGetters = createGetters(nodeIDs, &pg)
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

func (c clusterGetter) GetRows(limit int, queryID string) (resultChan chan cluster.RemoteQueryResult) {
	return c.gatherer.cluster.ExecuteRemotePullQuery(c.gatherer.serializedDag, queryID, limit, c.nodeID)
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

func SerializeDAG(dag PullExecutor, buffer []byte) ([]byte, error) {
	buffer, err := dag.Serialize(buffer)
	if err != nil {
		return nil, err
	}
	buffer = common.AppendUint32ToBufferLittleEndian(buffer, uint32(len(dag.GetChildren())))
	for _, child := range dag.GetChildren() {
		buffer, err = child.Serialize(buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func DeserializeDAG(buffer []byte, offset int) (PullExecutor, int, error) {
	executorType := ExecutorType(readInt(buffer, offset))
	offset += 4
	pullExecutor, err := CreateExecutor(executorType)
	if err != nil {
		return nil, 0, err
	}
	numChildren := readInt(buffer, offset)
	offset += 4
	for i := 0; i < numChildren; i++ {
		var child PullExecutor
		child, offset, err = DeserializeDAG(buffer, offset)
		if err != nil {
			return nil, 0, err
		}
		pullExecutor.AddChild(child)
	}
	return pullExecutor, offset, nil
}

func readInt(buffer []byte, offset int) int {
	intPtr := (*uint32)(unsafe.Pointer(&buffer[offset]))
	return int(*intPtr)
}
