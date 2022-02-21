/*
 *  Copyright 2022 Square Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dragon

import (
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/common"
	"io"
)

const (
	nodesStartedStateMachineUpdatedOK uint64 = 1
)

type nodesStartedSM struct {
	startSequences map[uint64]uint64 // node_id -> sequence_number
}

func (n *nodesStartedSM) Update(bytes []byte) (statemachine.Result, error) {
	nodeID, _ := common.ReadUint64FromBufferBE(bytes, 0)
	seq, ok := n.startSequences[nodeID]
	if !ok {
		seq = 0
	} else {
		seq++
	}
	n.startSequences[nodeID] = seq
	return statemachine.Result{
		Value: nodesStartedStateMachineUpdatedOK,
	}, nil
}

func (n *nodesStartedSM) Lookup(i interface{}) (interface{}, error) {
	return n.mapToBytes(), nil
}

func (n *nodesStartedSM) mapToBytes() []byte {
	var bytes []byte
	bytes = common.AppendUint64ToBufferBE(bytes, uint64(len(n.startSequences)))
	for nid, seq := range n.startSequences {
		bytes = common.AppendUint64ToBufferBE(bytes, nid)
		bytes = common.AppendUint64ToBufferBE(bytes, seq)
	}
	return bytes
}

func (n *nodesStartedSM) SaveSnapshot(writer io.Writer, collection statemachine.ISnapshotFileCollection, i <-chan struct{}) error {
	_, err := writer.Write(n.mapToBytes())
	return err
}

func (n *nodesStartedSM) RecoverFromSnapshot(reader io.Reader, files []statemachine.SnapshotFile, i <-chan struct{}) error {
	var bytes []byte
	for len(bytes) < 8 {
		readBuff := make([]byte, 8)
		n, err := reader.Read(readBuff)
		if err != nil {
			return err
		}
		bytes = append(bytes, readBuff[:n]...)
	}
	n.startSequences = bytesToMap(bytes)
	return nil
}

func bytesToMap(bytes []byte) map[uint64]uint64 {
	offset := 0
	var l, nid, seq uint64
	l, offset = common.ReadUint64FromBufferBE(bytes, offset)
	m := make(map[uint64]uint64, l)
	for i := 0; i < int(l); i++ {
		nid, offset = common.ReadUint64FromBufferBE(bytes, offset)
		seq, offset = common.ReadUint64FromBufferBE(bytes, offset)
		m[nid] = seq
	}
	return m
}

func (n *nodesStartedSM) Close() error {
	return nil
}

func (d *Dragon) newNodesStartedSM(_ uint64, _ uint64) statemachine.IStateMachine {
	return &nodesStartedSM{startSequences: make(map[uint64]uint64)}
}
