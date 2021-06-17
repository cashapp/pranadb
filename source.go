package pranadb

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	"github.com/squareup/pranadb/storage"
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
	Table         common.Table
	TopicInfo     *TopicInfo
	TableExecutor *exec.TableExecutor
}

func NewSource(name string, schemaName string, sourceTableID uint64, columnNames []string,
	columnTypes []common.ColumnType, pkCols []int, topicInfo *TopicInfo, storage storage.Storage) (*Source, error) {

	tableInfo := common.TableInfo{
		ID:             sourceTableID,
		TableName:      name,
		ColumnNames:    columnNames,
		ColumnTypes:    columnTypes,
		PrimaryKeyCols: pkCols,
	}

	sourceTable, err := common.NewTable(storage, &tableInfo)
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
	}, nil
}

func (s *Source) AddConsumingExecutor(node exec.PushExecutor) {
	s.TableExecutor.AddConsumingNode(node)
}
