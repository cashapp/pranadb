package sql

import (
	"github.com/squareup/pranadb/storage"
)

type Table interface {
	Info() *TableInfo

	Upsert(row *Row, writeBatch *storage.WriteBatch) error

	Delete(row *Row, writeBatch *storage.WriteBatch) error

	LookupInPk(key *Key) (*Row, error)

	TableScan(startKey *Key, endKey *Key, limit int) ([]*Row, error)

	IndexScan(indexName string, startKey *Key, endKey *Key, limit int, )
}

type table struct {
	storage  storage.Storage
	info     *TableInfo
	colTypes []ColumnType
}

func NewTable(storage storage.Storage, info *TableInfo) Table {
	return &table{storage: storage, info: info, colTypes: info.ColumnTypes}
}

func (t *table) Info() *TableInfo {
	return t.info
}

// What about partitions? What about table_name in key?
func (t *table) Upsert(row *Row, writeBatch *storage.WriteBatch) error {
	keyBuffPtr, err := t.encodeKey(row)
	if err != nil {
		return err
	}
	var valueBuff []byte
	valueBuffPtr, err := EncodeRow(row, t.colTypes, &valueBuff)
	if err != nil {
		return err
	}
	valueBuff = *valueBuffPtr
	kvPair := storage.KVPair{
		Key:   *keyBuffPtr,
		Value: valueBuff,
	}
	writeBatch.AddPut(kvPair)
	return nil
}

func (t *table) Delete(row *Row, writeBatch *storage.WriteBatch) error {
	keyBuffPtr, err := t.encodeKey(row)
	if err != nil {
		return err
	}
	writeBatch.AddDelete(*keyBuffPtr)
	return nil
}

func (t *table) encodeKey(row *Row) (*[]byte, error) {
	var keyBuff []byte
	keyBuffPtr, err := EncodeCols(row, t.info.PrimaryKeyCols, t.colTypes, &keyBuff)
	if err != nil {
		return nil, err
	}
	return keyBuffPtr, nil
}

func (t *table) LookupInPk(key *Key) (*Row, error) {
	panic("implement me")
}

func (t *table) TableScan(startKey *Key, endKey *Key, limit int) ([]*Row, error) {
	panic("implement me")
}

func (t *table) IndexScan(indexName string, startKey *Key, endKey *Key, limit int) {
	panic("implement me")
}
