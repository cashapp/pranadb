package common

// System table ids
const (
	ForwarderTableID            = 1
	ForwarderSequenceTableID    = 2
	ReceiverTableID             = 3
	ReceiverIngestTableID       = 4
	ReceiverSequenceTableID     = 5
	SequenceGeneratorTableID    = 6
	LocksTableID                = 7
	LastLogIndexReceivedTableID = 8
	SyncTableID                 = 9
	SchemaTableID               = 10 // SchemaTableID stores table schemas
	ProtobufTableID             = 11
	IndexTableID                = 12
	ToDeleteTableID             = 13
	LocalConfigTableID          = 14
	ForwardDedupTableID         = 15
	UserTableIDBase             = 1000
)
