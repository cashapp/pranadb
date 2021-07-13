package common

type SeqGenerator interface {
	GenerateSequence() uint64
}
