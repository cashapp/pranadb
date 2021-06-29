package push

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/storage"
	"log"
)

type source struct {
	sourceInfo          *common.SourceInfo
	tableExecutor       *exec.TableExecutor
	store               storage.Storage
	mover               *mover
	sharder             common.Sharder
	localShards         []uint64
	nextLocalShardIndex int
}

func (p *PushEngine) CreateSource(sourceInfo *common.SourceInfo) error {

	colTypes := sourceInfo.TableInfo.ColumnTypes

	tableExecutor, err := exec.NewTableExecutor(colTypes, sourceInfo.TableInfo, p.storage)
	if err != nil {
		return nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	source := source{
		sourceInfo:    sourceInfo,
		tableExecutor: tableExecutor,
		store:         p.storage,
		mover:         p.mover,
		sharder:       p,
		localShards:   genLocalShards(p.schedulers),
	}

	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil
	}
	rc := &remoteConsumer{
		RowsFactory: rf,
		ColTypes:    colTypes,
		RowsHandler: source.tableExecutor,
	}
	p.remoteConsumers[sourceInfo.TableInfo.ID] = rc

	p.sources[sourceInfo.TableInfo.ID] = &source

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

func genLocalShards(schedulers map[uint64]*shardScheduler) []uint64 {
	var localShards []uint64
	for shardID, _ := range schedulers {
		localShards = append(localShards, shardID)
	}
	return localShards
}

func (s *source) addConsumingExecutor(executor exec.PushExecutor) {
	s.tableExecutor.AddConsumingNode(executor)
}

func (s *source) ingestRows(rows *common.Rows) error {
	// We choose a local shard to handle this batch - rows are written into forwarder queue
	// and forwarded to dest shards
	shardIndex := s.nextLocalShardIndex
	// Doesn't matter if accessed concurrently an index update non atomic - this is best effort
	s.nextLocalShardIndex++
	if s.nextLocalShardIndex == len(s.localShards) {
		s.nextLocalShardIndex = 0
	}
	shardID := s.localShards[shardIndex]
	err := s.doIngest(rows, shardID)
	if err != nil {
		return err
	}
	return s.mover.PollForForwards(shardID)
}

func (s *source) doIngest(rows *common.Rows, shardID uint64) error {

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
		destShardID, err := s.sharder.CalculateShard(key)
		if err != nil {
			return err
		}
		// TODO we can consider an optimisation where execute on any local shards directly
		err = s.mover.QueueForRemoteSend(key, destShardID, &row, shardID, tableID, colTypes, batch)
		if err != nil {
			return err
		}
	}

	return s.store.WriteBatch(batch, true)
}
