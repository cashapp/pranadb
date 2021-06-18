package pranadb

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	"github.com/squareup/pranadb/storage"
	"github.com/squareup/pranadb/table"
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
	SchemaName    string
	Name          string
	Table         table.Table
	TopicInfo     *TopicInfo
	TableExecutor *exec.TableExecutor
	store         storage.Storage
	mover         *Mover
}

func NewSource(name string, schemaName string, sourceTableID uint64, columnNames []string,
	columnTypes []common.ColumnType, pkCols []int, topicInfo *TopicInfo, storage storage.Storage,
	mover *Mover) (*Source, error) {

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
	}, nil
}

func (s *Source) AddConsumingExecutor(node exec.PushExecutor) {
	s.TableExecutor.AddConsumingNode(node)
}

func (s *Source) IngestRows(rows *common.PushRows, shardID uint64) error {
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

		// Send the row to the aggregator node which most likely lives in a different shard
		err = s.mover.QueueForRemoteSend(key, &row, shardID, tableID, colTypes, batch)
		if err != nil {
			return err
		}
	}
	return s.store.WriteBatch(batch, true)
}
