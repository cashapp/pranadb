package table

import (
	"log"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
)

func Upsert(tableInfo *common.TableInfo, row *common.Row, writeBatch *storage.WriteBatch) error {
	keyBuff, err := encodeKeyFromRow(tableInfo, row, writeBatch.ShardID)
	if err != nil {
		return err
	}
	var valueBuff []byte
	valueBuff, err = common.EncodeRow(row, tableInfo.ColumnTypes, valueBuff)
	if err != nil {
		return err
	}
	writeBatch.AddPut(keyBuff, valueBuff)
	return nil
}

func Delete(tableInfo *common.TableInfo, row *common.Row, writeBatch *storage.WriteBatch) error {
	keyBuff, err := encodeKeyFromRow(tableInfo, row, writeBatch.ShardID)
	if err != nil {
		return err
	}
	writeBatch.AddDelete(keyBuff)
	return nil
}

func LookupInPk(tableInfo *common.TableInfo, key common.Key, keyColIndexes []int, shardID uint64, rowsFactory *common.RowsFactory, storage storage.Storage) (*common.Row, error) {
	buffer, err := encodeKey(tableInfo, key, keyColIndexes, shardID)
	if err != nil {
		return nil, err
	}

	log.Printf("Looking up key %v in table %d", buffer, tableInfo.ID)
	buffRes, err := storage.Get(shardID, buffer, true)
	if err != nil {
		return nil, err
	}
	if buffRes == nil {
		return nil, nil
	}
	log.Printf("Got k:%v v:%v", buffer, buffRes)

	rows := rowsFactory.NewRows(1)
	err = common.DecodeRow(buffRes, tableInfo.ColumnTypes, rows)
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
func LocalNodeTableScan(tableInfo *common.TableInfo, limit int, rowsFactory *common.RowsFactory, storage storage.Storage) (*common.Rows, error) {

	prefix := make([]byte, 0, 8)
	prefix = common.AppendUint64ToBufferLittleEndian(prefix, tableInfo.ID)

	kvPairs, err := storage.Scan(prefix, prefix, limit)
	if err != nil {
		return nil, err
	}
	rows := rowsFactory.NewRows(len(kvPairs))
	for _, kvPair := range kvPairs {
		err := common.DecodeRow(kvPair.Value, tableInfo.ColumnTypes, rows)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func encodeKeyPrefix(tableID uint64, shardID uint64) []byte {
	keyBuff := make([]byte, 0, 32)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, tableID)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, shardID)
	return keyBuff
}

func encodeKeyFromRow(tableInfo *common.TableInfo, row *common.Row, shardID uint64) ([]byte, error) {
	keyBuff := encodeKeyPrefix(tableInfo.ID, shardID)
	return common.EncodeCols(row, tableInfo.PrimaryKeyCols, tableInfo.ColumnTypes, keyBuff)
}

func encodeKey(tableInfo *common.TableInfo, key common.Key, keyColIndexes []int, shardID uint64) ([]byte, error) {
	keyBuff := encodeKeyPrefix(tableInfo.ID, shardID)
	return common.EncodeKey(key, tableInfo.ColumnTypes, keyColIndexes, keyBuff)
}
