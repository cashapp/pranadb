package pranadb

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	"github.com/squareup/pranadb/storage"
	"github.com/squareup/pranadb/table"
	"log"
)

type Encoding int

const (
	EncodingJSON Encoding = iota + 1
	EncodingProtobuf
	EncodingRaw
)

type TopicInfo struct {
	brokerName string
	topicName  string
	keyFormat  Encoding
	properties map[string]interface{}
}

type Source struct {
	SchemaName          string
	Name                string
	Table               table.Table
	TopicInfo           *TopicInfo
	TableExecutor       *exec.TableExecutor
	store               storage.Storage
	mover               *Mover
	sharder             common.Sharder
	localShards         []uint64
	nextLocalShardIndex int
}

func NewSource(name string, schemaName string, sourceTableID uint64, columnNames []string,
	columnTypes []common.ColumnType, pkCols []int, topicInfo *TopicInfo, storage storage.Storage,
	mover *Mover, sharder common.Sharder, schedulers map[uint64]*ShardScheduler) (*Source, error) {

	tableInfo := common.TableInfo{
		ID:             sourceTableID,
		TableName:      name,
		ColumnNames:    columnNames,
		ColumnTypes:    columnTypes,
		PrimaryKeyCols: pkCols,
	}

	sourceTable, err := table.NewTable(storage, &tableInfo)
	if err != nil {
		return nil, err
	}

	tableExecutor, err := exec.NewTableExecutor(columnTypes, sourceTable, storage)
	if err != nil {
		return nil, err
	}

	return &Source{
		SchemaName:    schemaName,
		Name:          name,
		Table:         sourceTable,
		TopicInfo:     topicInfo,
		TableExecutor: tableExecutor,
		store:         storage,
		mover:         mover,
		sharder:       sharder,
		localShards:   genLocalShards(schedulers),
	}, nil
}

// SetSchedulers is called by the PranaNode whenever the shards change
// TODO make sure all processing is complete before changing the schedulers
// TODO atomic references
func (s *Source) SetSchedulers(schedulers map[uint64]*ShardScheduler) {
	// Set the local shards too
}

func genLocalShards(schedulers map[uint64]*ShardScheduler) []uint64 {
	var localShards []uint64
	for shardID, _ := range schedulers {
		localShards = append(localShards, shardID)
	}
	return localShards
}

func (s *Source) AddConsumingExecutor(node exec.PushExecutor) {
	s.TableExecutor.AddConsumingNode(node)
}

func (s *Source) IngestRows(rows *common.PushRows) error {
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
	return s.mover.pollForForwards(shardID)
}

func (s *Source) doIngest(rows *common.PushRows, shardID uint64) error {

	log.Printf("Ingesting rows on shard %d", shardID)

	// TODO where source has no key - need to create one

	// Partition the rows and send them to the appropriate shards
	pkCols := s.Table.Info().PrimaryKeyCols
	colTypes := s.Table.Info().ColumnTypes
	tableID := s.Table.Info().ID
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
