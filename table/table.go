package table

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

func Upsert(tableInfo *common.TableInfo, row *common.Row, writeBatch *cluster.WriteBatch) error {
	keyBuff, err := encodeKeyFromRow(tableInfo, row, writeBatch.ShardID)
	if err != nil {
		return errors.WithStack(err)
	}
	var valueBuff []byte
	valueBuff, err = common.EncodeRow(row, tableInfo.ColumnTypes, valueBuff)
	if err != nil {
		return errors.WithStack(err)
	}
	writeBatch.AddPut(keyBuff, valueBuff)
	return nil
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
