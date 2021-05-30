package storage

type WriteBatch struct {

}

type Storage interface {

	writeBatch(partitionNumber uint64, batch *WriteBatch, localLeader bool) error

	get(partitionNumber uint64, key []byte) ([]byte, error)

	scan(partitionNumber uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([][]byte, error)
}

type storage struct {


}

func (s *storage) writeBatch(partitionNumber uint64, batch *WriteBatch, localLeader bool) error {
	panic("implement me")
}

func (s *storage) get(partitionNumber uint64, key []byte) ([]byte, error) {
	panic("implement me")
}

func (s *storage) scan(partitionNumber uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([][]byte, error) {
	panic("implement me")
}
