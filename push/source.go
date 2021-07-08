package push

import (
	"log"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/storage"
)

type source struct {
	sourceInfo    *common.SourceInfo
	tableExecutor *exec.TableExecutor
	engine        *PushEngine
}

func (p *PushEngine) CreateSource(sourceInfo *common.SourceInfo) error {

	colTypes := sourceInfo.TableInfo.ColumnTypes

	tableExecutor := exec.NewTableExecutor(colTypes, sourceInfo.TableInfo, p.storage)

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

func (p *PushEngine) RemoveSource(sourceID uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.sources, sourceID)
	delete(p.remoteConsumers, sourceID)
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

func (s *source) ingestRows(rows *common.Rows, shardID uint64) error {

	log.Printf("Ingesting rows on shard %d", shardID)

	// TODO where source has no key - need to create one

	// Partition the rows and send them to the appropriate shards
	info := s.sourceInfo.TableInfo
	pkCols := info.PrimaryKeyCols
	colTypes := info.ColumnTypes
	tableID := info.ID
	batch := storage.NewWriteBatch(shardID)

	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		key := make([]byte, 0, 8)
		key, err := common.EncodeCols(&row, pkCols, colTypes, key)
		if err != nil {
			return err
		}
		destShardID, err := s.engine.sharder.CalculateShard(sharder.ShardTypeHash, key)
		if err != nil {
			return err
		}
		// TODO we can consider an optimisation where execute on any local shards directly
		err = s.engine.QueueForRemoteSend(key, destShardID, &row, shardID, tableID, colTypes, batch)
		if err != nil {
			return err
		}
	}

	err := s.engine.storage.WriteBatch(batch, true)
	if err != nil {
		return err
	}
	return s.engine.transferData(shardID, true)
}
