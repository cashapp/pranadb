package pranadb

func ComputeShard(key []byte, shardIDs []uint64) uint64 {
	hash := hash(key)

	index := hash % uint32(len(shardIDs))
	return shardIDs[index]
}

func hash(key []byte) uint32 {

	// TODO consistent hashing
	hash := uint32(31)
	for _, b := range key {
		hash = 31*hash + uint32(b)
	}
	return hash
}
