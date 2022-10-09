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

package util

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser/selector"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/push/codec"
)

func EncodeKeyForForwardIngest(sourceID uint64, partitionID uint64, offset uint64, remoteConsumerID uint64) []byte {
	buff := make([]byte, 0, 32)
	// The first 24 bytes is the dedup key and comprises [originator_id (16 bytes), sequence (8 bytes)]
	// Originator id for an ingest from Kafka comprises [source_id (8 bytes), partition_id (8 bytes) ]
	// The sequence is the offset in the Kafka partition
	buff = common.AppendUint64ToBufferBE(buff, sourceID)
	buff = common.AppendUint64ToBufferBE(buff, partitionID)
	buff = common.AppendUint64ToBufferBE(buff, offset)

	// And remote consumer id goes on the end
	buff = common.AppendUint64ToBufferBE(buff, remoteConsumerID)
	return buff
}

func EncodeKeyForForwardAggregation(originatorTableID uint64, sendingShardID uint64,
	sequence uint64, remoteConsumerID uint64) []byte {

	buff := make([]byte, 0, 32)

	// The next 24 bytes is the dedup key and comprises [originator_id (16 bytes), sequence (8 bytes)]
	// Originator id for forward of partial aggregation is [agg_table_id (8 bytes), sending_shard_id (8 bytes) ]
	// The sequence is the receiver sequence
	buff = common.AppendUint64ToBufferBE(buff, originatorTableID)
	buff = common.AppendUint64ToBufferBE(buff, sendingShardID)
	buff = common.AppendUint64ToBufferBE(buff, sequence)
	// And remote consumer id goes on the end
	buff = common.AppendUint64ToBufferBE(buff, remoteConsumerID)

	return buff
}

func EncodePrevAndCurrentRow(prevValueBuff []byte, currValueBuff []byte) []byte {
	lpvb := len(prevValueBuff)
	lcvb := len(currValueBuff)
	buff := make([]byte, 0, lpvb+lcvb+8)
	buff = common.AppendUint32ToBufferLE(buff, uint32(lpvb))
	buff = append(buff, prevValueBuff...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(lcvb))
	buff = append(buff, currValueBuff...)
	return buff
}

func SendForwardBatches(forwardBatches map[uint64]*cluster.WriteBatch, clust cluster.Cluster, direct bool, fill bool) error {
	lb := len(forwardBatches)
	chs := make([]chan error, 0, lb)
	for _, b := range forwardBatches {
		ch := make(chan error, 1)
		chs = append(chs, ch)
		theBatch := b
		go func() {
			if err := clust.WriteForwardBatch(theBatch, direct, fill); err != nil {
				ch <- err
				return
			}
			ch <- nil
		}()
	}
	for i := 0; i < lb; i++ {
		err, ok := <-chs[i]
		if !ok {
			panic("channel closed")
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func CopyAndAddAllProperties(p1 map[string]string, p2 map[string]string) map[string]string {
	m := make(map[string]string, len(p1)+len(p2))
	for k, v := range p2 {
		m[k] = v
	}
	// p1 properties override p2 so we add them last
	for k, v := range p1 {
		m[k] = v
	}
	return m
}

func GetCodecs(registry protolib.Resolver, headerEncoding common.KafkaEncoding, keyEncoding common.KafkaEncoding, valueEncoding common.KafkaEncoding,
	selectorInjectors []selector.ColumnSelector) (headerCodec codec.Codec, keyCodec codec.Codec, valueCodec codec.Codec, err error) {
	// We pre-compute whether the selectors need headers, key and value so we don't unnecessary parse them if they
	// don't use them
	var (
		decodeHeader, decodeKey, decodeValue bool
	)
	for _, selector := range selectorInjectors {
		metaKey := selector.MetaKey
		if metaKey == nil {
			decodeValue = true
		} else {
			switch *metaKey {
			case "header":
				decodeHeader = true
			case "key":
				decodeKey = true
			case "timestamp":
				// timestamp selector, no decoding required
			default:
				panic(fmt.Sprintf("invalid selector %q", selector))
			}
			if err != nil {
				return
			}
		}
	}
	if decodeHeader {
		headerCodec, err = codec.GetCodec(registry, headerEncoding)
		if err != nil {
			return
		}
	}
	if decodeKey {
		keyCodec, err = codec.GetCodec(registry, keyEncoding)
		if err != nil {
			return
		}
	}
	if decodeValue {
		valueCodec, err = codec.GetCodec(registry, valueEncoding)
		if err != nil {
			return
		}
	}
	return headerCodec, keyCodec, valueCodec, err
}
