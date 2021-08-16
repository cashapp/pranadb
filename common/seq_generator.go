package common

type SeqGenerator interface {
	GenerateSequence() uint64
}

// PreAllocatedSeqGenerator is a sequence generator that enumerates a fixed, already obtained sequence IDs.
// We need to reserve the table sequences required for the DDL statement *before* we broadcast the DDL across the
// cluster, and those same table sequence values have to be used on every node for consistency.
type PreAllocatedSeqGenerator struct {
	sequences []uint64
	index     int
}

func NewPreallocSeqGen(seq []uint64) *PreAllocatedSeqGenerator {
	return &PreAllocatedSeqGenerator{
		sequences: seq,
		index:     0,
	}
}

func (p *PreAllocatedSeqGenerator) GenerateSequence() uint64 {
	if p.index >= len(p.sequences) {
		panic("not enough sequence values")
	}
	res := p.sequences[p.index]
	p.index++
	return res
}
