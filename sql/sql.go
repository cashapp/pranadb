package sql

type Row struct {

}

type Key struct {

}

type SQLManager interface {

	lookupInPK(key Key) (Row, error)

	lookupInUniqueIndex(indexID uint64, key Key) (Row, error)

	lookupInNonUniqueIndex(indexID uint64, key Key) ([]Row, error)

	store(tableID uint64, row Row) error

}
