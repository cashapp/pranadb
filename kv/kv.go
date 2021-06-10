package kv

// Low level KV storage interface
type KV interface {
	put(key []byte, value []byte) error

	get(key []byte) ([]byte, error)

	scan(startKeyPrefix []byte, endKeyPrefix []byte) ([][]byte, error)
}
