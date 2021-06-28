package table

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
	"log"
)

// TODO get rid of this and add encoding methods to common encoding
type Table interface {
	Info() *common.TableInfo

	Upsert(row *common.Row, writeBatch *storage.WriteBatch) error

	Delete(row *common.Row, writeBatch *storage.WriteBatch) error

	LookupInPk(key common.Key, shardID uint64) (*common.Row, error)

	LocalNodeTableScan(limit int) (*common.Rows, error)

	IndexScan(indexName string, startKey *common.Key, whileKey *common.Key, limit int, shardID uint64) (*common.Rows, error)
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

func (t *table) Upsert(row *common.Row, writeBatch *storage.WriteBatch) error {
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

func (t *table) Delete(row *common.Row, writeBatch *storage.WriteBatch) error {
	keyBuff, err := t.encodeKeyFromRow(row, writeBatch.ShardID)
	if err != nil {
		return err
	}
	writeBatch.AddDelete(keyBuff)
	return nil
}

func (t *table) LookupInPk(key common.Key, shardID uint64) (*common.Row, error) {
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

// LocalNodeTableScan returns all rows for all shards in the local node
func (t *table) LocalNodeTableScan(limit int) (*common.Rows, error) {

	prefix := make([]byte, 0, 8)
	prefix = common.AppendUint64ToBufferLittleEndian(prefix, t.info.ID)

	kvPairs, err := t.storage.Scan(prefix, prefix, limit)
	if err != nil {
		return nil, err
	}
	rows := t.rowsFactory.NewRows(len(kvPairs))
	for _, kvPair := range kvPairs {
		err := common.DecodeRow(kvPair.Value, t.info.ColumnTypes, rows)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (t *table) IndexScan(indexName string, startKey *common.Key, endKey *common.Key, limit int, shardID uint64) (*common.Rows, error) {
	panic("implement me")
}

func (t *table) encodeKeyPrefix(shardID uint64) []byte {
	// Key is |shard_id|table_id|pk_value
	keyBuff := make([]byte, 0, 32)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, shardID)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, t.info.ID)
	return keyBuff
}

func (t *table) encodeKeyFromRow(row *common.Row, shardID uint64) ([]byte, error) {
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
