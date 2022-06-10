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

func EncodeIndexKeyValue(tableInfo *common.TableInfo, indexInfo *common.IndexInfo, shardID uint64, row *common.Row) ([]byte, []byte, error) {
	keyBuff := EncodeTableKeyPrefix(indexInfo.ID, shardID, 32)
	keyBuff, err := common.EncodeIndexKeyCols(row, indexInfo.IndexCols, tableInfo.ColumnTypes, keyBuff)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	pkStart := len(keyBuff)
	// We encode the PK cols on both the end of the key and the value
	// It needs to be on the key to make the entry unique (for non unique indexes)
	// and on the value so we can make looking up the PK easy for non covering indexes without having to parse the
	// whole key
	keyBuff, err = common.EncodeKeyCols(row, tableInfo.PrimaryKeyCols, tableInfo.ColumnTypes, keyBuff)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	valueBuff := keyBuff[pkStart:] // Value is just the PK
	return keyBuff, valueBuff, nil
}
