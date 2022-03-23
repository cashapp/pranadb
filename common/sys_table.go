package common

// System table ids
const (
	ForwarderTableID            = 1
	ForwarderSequenceTableID    = 2
	ReceiverTableID             = 3
	ReceiverSequenceTableID     = 4
	SequenceGeneratorTableID    = 5
	LocksTableID                = 6
	LastLogIndexReceivedTableID = 7
	SyncTableID                 = 8
	SchemaTableID               = 9 // SchemaTableID stores table schemas
	OffsetsTableID              = 10
	ProtobufTableID             = 11
	IndexTableID                = 12
	ToDeleteTableID             = 13
	LocalConfigTableID          = 14
	UserTableIDBase             = 1000
)
