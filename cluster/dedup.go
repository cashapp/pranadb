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

package cluster

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
)

/*
DoDedup checks for a duplicate key.

We use duplicate detection in PranaDB for two things:

*Screening out duplicate Kafka messages ingested in sources*

When we ingest a batch of messages from Kafka, we shard each message to find the destination shard, then for each
destination shard we write a batch into the receiver table for that shard. Failure can occur after having successfully
written to one or more destination shards but not all. We acknowledge the Kafka batch only after successfully writing to
all shards. On recovery a new batch of messages from the last committed offset are consumed again, sharded and forward
batches written to destination shards. We need to ignore any messages that have already been written.

*Screening out duplicate rows which are moved from one shard to another*

Sometimes in PranaDB, when processing rows through a materialized view we need to forward some data from one shard to
another. A classic example is when calculating an aggregation. Imagine a materialized view with a simple aggregation.
Data is being processed on a shard, the batch of rows reaches an aggregator they are forwarded to the shards that own
the data for the aggregation key (defined by the group by columns).

*How the mechanism works*

In both the above cases we need a way of writing rows to another shard such that rows will be ignored if they have been
seen before.

When we provide rows for writing into the shard state machine for the receiver table we also provide a dedup key for
each row, this is composed of:

originator_id: 16 bytes
sequence: 8 bytes

originator_id uniquely defines where the row came from - in the case of ingesting rows from Kafka it is made up of:

source_id: 8 bytes
partition_id: 8 bytes

When receiving a row from another shard (e.g. in the case of forwarding a partial aggregation) it is made of:

internal aggregation table id: 8 bytes
shard id (being forwarded from): 8 bytes

When the shard state machine receives a row with a particular dedup key, it looks up the last sequence value it received
from the same originator_id. If the previous sequence value >= the current one, that means it's a duplicate row and is
ignored.

When rows are actually written into the receiver table, the shard state machine writes the key as:

|shard_id|receiver_table_id|receiver_sequence|remote_consumer_id

Where do we get the sequence field in the dedup key from?

In the case of forwarding rows we have just consumed from Kafka, sequence is simply the offset in the Kafka partition.

In the case of forwarding rows in an aggregation to the owner of the key, the receiver sequence is used.
*/
func DoDedup(shardID uint64, dedupKey []byte, dedupMap map[string]uint64) (bool, error) {

	// Originator id is first 16 bytes
	oid := dedupKey[:16]
	soid := string(oid)

	// Sequence is next 8 bytes
	sequence, _ := common.ReadUint64FromBufferBE(dedupKey, 16)

	prevSequence, ok := dedupMap[soid]
	if ok {
		if prevSequence >= sequence {
			log.Warnf("Duplicate forward row detected in shard %d - originator id %v prev seq %d curr seq %d",
				shardID, oid, prevSequence, sequence)
			return true, nil
		}
	}
	dedupMap[soid] = sequence
	return false, nil
}
