package common

// System table ids
const (
	ReceiverTableID             = 1
	SequenceGeneratorTableID    = 2
	LocksTableID                = 3
	LastLogIndexReceivedTableID = 4
	SyncTableID                 = 5
	SchemaTableID               = 6 // SchemaTableID stores table schemas
	ProtobufTableID             = 7
	IndexTableID                = 8
	ToDeleteTableID             = 9
	LocalConfigTableID          = 10
	ForwardDedupTableID         = 11
	ShardLeaderTableID          = 12
	UserTableIDBase             = 1000
)
