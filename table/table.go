package table

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
)

func Upsert(tableInfo *common.TableInfo, row *common.Row, writeBatch *cluster.WriteBatch) error {
	if len(tableInfo.PrimaryKeyCols) == 0 {
		return fmt.Errorf("table %s has no primary key", tableInfo.Name)
	}
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

func Delete(tableInfo *common.TableInfo, row *common.Row, writeBatch *cluster.WriteBatch) error {
	keyBuff, err := encodeKeyFromRow(tableInfo, row, writeBatch.ShardID)
	if err != nil {
		return err
	}
	writeBatch.AddDelete(keyBuff)
	return nil
}

func LookupInPk(tableInfo *common.TableInfo, key common.Key, keyColIndexes []int, shardID uint64, rowsFactory *common.RowsFactory, storage cluster.Cluster) (*common.Row, error) {
	buffer, err := encodeKey(tableInfo, key, keyColIndexes, shardID)
	if err != nil {
		return nil, err
	}
	buffRes, err := storage.LocalGet(buffer)
	if err != nil {
		return nil, err
	}
	if buffRes == nil {
		return nil, nil
	}
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

func EncodeTableKeyPrefix(tableID uint64, shardID uint64, capac int) []byte {
	keyBuff := make([]byte, 0, capac)
	// Data key must be in big-endian order so that byte-by-byte key comparison correctly orders the keys
	keyBuff = common.AppendUint64ToBufferBE(keyBuff, shardID)
	keyBuff = common.AppendUint64ToBufferBE(keyBuff, tableID)
	return keyBuff
}

func encodeKeyFromRow(tableInfo *common.TableInfo, row *common.Row, shardID uint64) ([]byte, error) {
	keyBuff := EncodeTableKeyPrefix(tableInfo.ID, shardID, 32)
	return common.EncodeKeyCols(row, tableInfo.PrimaryKeyCols, tableInfo.ColumnTypes, keyBuff)
}

func encodeKey(tableInfo *common.TableInfo, key common.Key, keyColIndexes []int, shardID uint64) ([]byte, error) {
	keyBuff := EncodeTableKeyPrefix(tableInfo.ID, shardID, 32)
	return common.EncodeKey(key, tableInfo.ColumnTypes, keyColIndexes, keyBuff)
}
