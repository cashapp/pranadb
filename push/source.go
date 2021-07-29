package push

import (
	"log"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/sharder"
)

type source struct {
	sourceInfo    *common.SourceInfo
	tableExecutor *exec.TableExecutor
	engine        *PushEngine
}

func (p *PushEngine) CreateSource(sourceInfo *common.SourceInfo) error {

	colTypes := sourceInfo.TableInfo.ColumnTypes

	tableExecutor := exec.NewTableExecutor(colTypes, sourceInfo.TableInfo, p.cluster)

	p.lock.Lock()
	defer p.lock.Unlock()

	src := source{
		sourceInfo:    sourceInfo,
		tableExecutor: tableExecutor,
		engine:        p,
	}

	rf := common.NewRowsFactory(colTypes)
	rc := &remoteConsumer{
		RowsFactory: rf,
		ColTypes:    colTypes,
		RowsHandler: src.tableExecutor,
	}
	p.remoteConsumers[sourceInfo.TableInfo.ID] = rc

	p.sources[sourceInfo.TableInfo.ID] = &src

	return nil
}

// SetSchedulers is called by the PranaNode whenever the shards change
// TODO make sure all processing is complete before changing the schedulers
// TODO atomic references
func (s *source) setSchedulers(schedulers map[uint64]*shardScheduler) {
	// Set the local shards too
}

func (s *source) start() error {
	return nil
}

func (s *source) stop() error {
	return nil
}

func (s *source) addConsumingExecutor(executor exec.PushExecutor) {
	s.tableExecutor.AddConsumingNode(executor)
}

func (s *source) removeConsumingExecutor(executor exec.PushExecutor) {
	s.tableExecutor.RemoveConsumingNode(executor)
}

func (s *source) ingestRows(rows *common.Rows, shardID uint64) error {

	log.Printf("Ingesting rows on shard %d and node %d", shardID, s.engine.cluster.GetNodeID())

	// TODO where source has no key - need to create one

	// Partition the rows and send them to the appropriate shards
	info := s.sourceInfo.TableInfo
	pkCols := info.PrimaryKeyCols
	colTypes := info.ColumnTypes
	tableID := info.ID
	batch := cluster.NewWriteBatch(shardID, false)

	/*
		TODO idempotency
		If a batch of rows is committed to the forwarder queue for a shard, it's possible the call to write the batch
		returned an error but the batch was actually committed successfully across all replicas.
		In this case the Kafka offset won't have been committed, and when retried the same batch of rows would be
		forwarded again.
		To prevent this, we will need to store the [kafka_partition_id, kafka_offset] in the same write batch as the
		one to commit the rows to the forward queue.
		We will load this mapping on startup and maintain it in memory too, as rows come in we will check against
		last seen offset for a partition and if seen before we do not forward the row, but still commit the Kafka
		offset
	*/

	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		key := make([]byte, 0, 8)
		key, err := common.EncodeKeyCols(&row, pkCols, colTypes, key)
		if err != nil {
			return err
		}
		destShardID, err := s.engine.sharder.CalculateShard(sharder.ShardTypeHash, key)
		if err != nil {
			return err
		}
		// TODO we can consider an optimisation where execute on any local shards directly
		err = s.engine.QueueForRemoteSend(destShardID, &row, shardID, tableID, colTypes, batch)
		if err != nil {
			return err
		}
	}

	err := s.engine.cluster.WriteBatch(batch)
	if err != nil {
		return err
	}
	return s.engine.transferData(shardID, true)
}
