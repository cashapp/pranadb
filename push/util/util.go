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
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

func EncodeKeyForForwardIngest(sourceID uint64, partitionID uint64, offset uint64, remoteConsumerID uint64) []byte {
	buff := make([]byte, 0, 33)
	buff = append(buff, 1) // This byte means duplicated detection is enabled
	// The first 24 bytes is the dedup key and comprises [originator_id (16 bytes), sequence (8 bytes)]
	// Originator id for an ingest from Kafka comprises [source_id (8 bytes), partition_id (8 bytes) ]
	buff = common.AppendUint64ToBufferBE(buff, sourceID)
	buff = common.AppendUint64ToBufferBE(buff, partitionID)
	buff = common.AppendUint64ToBufferBE(buff, offset)

	// And remote consumer id goes on the end
	buff = common.AppendUint64ToBufferBE(buff, remoteConsumerID)
	return buff
}

func EncodeKeyForForwardAggregation(enableDupDetection bool, partialAggTableID uint64, sendingShardID uint64,
	sequence uint64, remoteConsumerID uint64) []byte {

	buff := make([]byte, 0, 33)

	// First byte is whether duplicate detection is enabled or not
	if enableDupDetection {
		buff = append(buff, 1)
	} else {
		buff = append(buff, 0)
	}

	// The next 24 bytes is the dedup key and comprises [originator_id (16 bytes), sequence (8 bytes)]
	// Originator id for forward of partial aggregation is [partial_agg_table_id (8 bytes), sending_shard_id (8 bytes) ]
	buff = common.AppendUint64ToBufferBE(buff, partialAggTableID)
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

func SendForwardBatches(forwardBatches map[uint64]*cluster.WriteBatch, clust cluster.Cluster) error {
	lb := len(forwardBatches)
	chs := make([]chan error, 0, lb)

	for _, b := range forwardBatches {
		ch := make(chan error, 1)
		chs = append(chs, ch)
		theBatch := b
		go func() {
			err := clust.WriteForwardBatch(theBatch)
			if err != nil {
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
