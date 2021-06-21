package table

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
	"log"
)

type Table interface {
	Info() *common.TableInfo

	Upsert(row *common.PushRow, writeBatch *storage.WriteBatch) error

	Delete(row *common.PushRow, writeBatch *storage.WriteBatch) error

	LookupInPk(key common.Key, shardID uint64) (*common.PushRow, error)

	TableScan(startKey *common.Key, endKey *common.Key, limit int) ([]*common.PushRow, error)

	IndexScan(indexName string, startKey *common.Key, endKey *common.Key, limit int)
}

type table struct {
	storage     storage.Storage
	info        *common.TableInfo
	rowsFactory *common.RowsFactory
	keyColTypes []common.ColumnType
}

func NewTable(storage storage.Storage, info *common.TableInfo) (Table, error) {
	rowsFactory, err := common.NewRowsFactory(info.ColumnTypes)
	if err != nil {
		return nil, err
	}
	keyColTypes := make([]common.ColumnType, len(info.PrimaryKeyCols))
	for i, pkCol := range info.PrimaryKeyCols {
		keyColTypes[i] = info.ColumnTypes[pkCol]
	}
	return &table{
		storage:     storage,
		info:        info,
		rowsFactory: rowsFactory,
		keyColTypes: keyColTypes}, nil
}

func (t *table) Info() *common.TableInfo {
	return t.info
}

func (t *table) Upsert(row *common.PushRow, writeBatch *storage.WriteBatch) error {
	keyBuff, err := t.encodeKeyFromRow(row, writeBatch.ShardID)
	if err != nil {
		return err
	}
	var valueBuff []byte
	valueBuff, err = common.EncodeRow(row, t.info.ColumnTypes, valueBuff)
	if err != nil {
		return err
	}
	writeBatch.AddPut(keyBuff, valueBuff)

	log.Printf("upserting k:%v v:%v", keyBuff, valueBuff)

	return nil
}

func (t *table) Delete(row *common.PushRow, writeBatch *storage.WriteBatch) error {
	keyBuff, err := t.encodeKeyFromRow(row, writeBatch.ShardID)
	if err != nil {
		return err
	}
	writeBatch.AddDelete(keyBuff)
	return nil
}

func (t *table) LookupInPk(key common.Key, shardID uint64) (*common.PushRow, error) {
	buffer, err := t.encodeKey(key, shardID)
	if err != nil {
		return nil, err
	}

	log.Printf("Looking up key %v in table %d", buffer, t.info.ID)
	buffRes, err := t.storage.Get(shardID, buffer, true)
	if err != nil {
		return nil, err
	}
	if buffRes == nil {
		return nil, nil
	}
	fmt.Printf("Getting k:%s v:%s", string(buffer), string(buffRes))

	rows := t.rowsFactory.NewRows(1)
	err = common.DecodeRow(buffRes, t.info.ColumnTypes, rows)
	if err != nil {
		return nil, err
	}
	if rows.RowCount() != 1 {
		panic("expected one row")
	}
	row := rows.GetRow(0)
	return &row, nil
}

func (t *table) TableScan(startKey *common.Key, endKey *common.Key, limit int) ([]*common.PushRow, error) {
	panic("implement me")
}

func (t *table) IndexScan(indexName string, startKey *common.Key, endKey *common.Key, limit int) {
	panic("implement me")
}

func (t *table) encodeKeyPrefix(shardID uint64) []byte {
	// Key is |shard_id|table_id|pk_value
	keyBuff := make([]byte, 0, 32)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, shardID)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, t.info.ID)
	return keyBuff
}

func (t *table) encodeKeyFromRow(row *common.PushRow, shardID uint64) ([]byte, error) {
	keyBuff := t.encodeKeyPrefix(shardID)
	keyBuff, err := common.EncodeCols(row, t.info.PrimaryKeyCols, t.info.ColumnTypes, keyBuff)
	if err != nil {
		return nil, err
	}
	return keyBuff, nil
}

func (t *table) encodeKey(key common.Key, shardID uint64) ([]byte, error) {
	keyBuff := t.encodeKeyPrefix(shardID)
	keyBuff, err := common.EncodeKey(key, t.keyColTypes, keyBuff)
	if err != nil {
		return nil, err
	}
	return keyBuff, nil
}
