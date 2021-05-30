package kv

type Pebble struct {
}

func (p Pebble) put(key []byte, value []byte) error {
	panic("implement me")
}

func (p Pebble) get(key []byte) ([]byte, error) {
	panic("implement me")
}

func (p Pebble) scan(startPrefix []byte, endPrefix []byte) ([][]byte, error) {
	panic("implement me")
}



