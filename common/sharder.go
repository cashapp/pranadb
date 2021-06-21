package common

type Sharder interface {
	CalculateShard(key []byte) (uint64, error)
}
