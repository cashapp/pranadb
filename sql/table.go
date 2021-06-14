package sql

import (
	"fmt"
	"github.com/squareup/pranadb/storage"
)

type Table interface {
	Info() *TableInfo

	Upsert(row *PullRow, writeBatch *storage.WriteBatch) error

	Delete(row *PullRow, writeBatch *storage.WriteBatch) error

	LookupInPk(key Key, shardID uint64) (*PullRow, error)

	TableScan(startKey *Key, endKey *Key, limit int) ([]*PullRow, error)

	IndexScan(indexName string, startKey *Key, endKey *Key, limit int)
}

type table struct {
	storage     storage.Storage
	info        *TableInfo
	rowsFactory *RowsFactory
}

func NewTable(storage storage.Storage, info *TableInfo) (Table, error) {
	rowsFactory, err := NewRowsFactory(info.ColumnTypes)
	if err != nil {
		return nil, err
	}
	return &table{storage: storage, info: info, rowsFactory: rowsFactory}, nil
}

func (t *table) Info() *TableInfo {
	return t.info
}

func (t *table) Upsert(row *PullRow, writeBatch *storage.WriteBatch) error {
	keyBuff, err := t.encodeKeyFromRow(row, writeBatch.ShardID)
	if err != nil {
		return err
	}
	var valueBuff []byte
	valueBuff, err = EncodeRow(row, t.info.ColumnTypes, valueBuff)
	if err != nil {
		return err
	}
	kvPair := storage.KVPair{
		Key:   keyBuff,
		Value: valueBuff,
	}
	writeBatch.AddPut(kvPair)

	fmt.Sprintf("upserting k:%s v:%s", string(keyBuff), string(valueBuff))

	return nil
}

func (t *table) Delete(row *PullRow, writeBatch *storage.WriteBatch) error {
	keyBuff, err := t.encodeKeyFromRow(row, writeBatch.ShardID)
	if err != nil {
		return err
	}
	writeBatch.AddDelete(keyBuff)
	return nil
}

func (t *table) LookupInPk(key Key, shardID uint64) (*PullRow, error) {
	buffer, err := t.encodeKey(key, shardID)
	if err != nil {
		return nil, err
	}
	buffRes, err := t.storage.Get(shardID, buffer)
	if err != nil {
		return nil, err
	}
	if buffRes == nil {
		return nil, nil
	}
	fmt.Printf("Getting k:%s v:%s", string(buffer), string(buffRes))

	rows := t.rowsFactory.NewRows(1)
	err = DecodeRow(buffRes, t.info.ColumnTypes, rows)
	if err != nil {
		return nil, err
	}
	if rows.RowCount() != 1 {
		panic("expected one row")
	}
	row := rows.GetRow(0)
	return &row, nil
}

func (t *table) TableScan(startKey *Key, endKey *Key, limit int) ([]*PullRow, error) {
	panic("implement me")
}

func (t *table) IndexScan(indexName string, startKey *Key, endKey *Key, limit int) {
	panic("implement me")
}

func (t *table) encodeKeyPrefix(shardID uint64) []byte {
	// Key is |table_id|shard_id|pk_value
	keyBuff := make([]byte, 0, 32)
	keyBuff = appendUint64ToBufferLittleEndian(keyBuff, t.info.ID)
	keyBuff = appendUint64ToBufferLittleEndian(keyBuff, shardID)
	return keyBuff
}

func (t *table) encodeKeyFromRow(row *PullRow, shardID uint64) ([]byte, error) {
	keyBuff := t.encodeKeyPrefix(shardID)
	keyBuff, err := EncodeCols(row, t.info.PrimaryKeyCols, t.info.ColumnTypes, keyBuff)
	if err != nil {
		return nil, err
	}
	return keyBuff, nil
}

func (t *table) encodeKey(key Key, shardID uint64) ([]byte, error) {
	keyBuff := t.encodeKeyPrefix(shardID)
	keyBuff, err := EncodeKey(key, t.info.ColumnTypes, keyBuff)
	if err != nil {
		return nil, err
	}
	return keyBuff, nil
}
