package load

import (
	json2 "encoding/json"
	"fmt"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/kafka"
	"math/rand"
	"time"
)

type simpleGenerator struct {
	uniqueIDsPerPartition int64
}

func (s *simpleGenerator) GenerateMessage(partitionID int32, offset int64, rnd *rand.Rand) (*kafka.Message, error) {
	m := make(map[string]interface{})
	customerToken := fmt.Sprintf("customer-token-%d-%d", partitionID, offset%s.uniqueIDsPerPartition)
	m["primary_key_field"] = customerToken
	m["varchar_field"] = fmt.Sprintf("customer-full-name-%s", customerToken)
	m["bigint_field"] = offset % 1000

	json, err := json2.Marshal(&m)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	msg := &kafka.Message{
		Key:       []byte(customerToken),
		Value:     json,
		TimeStamp: time.Now(),
		PartInfo: kafka.PartInfo{
			PartitionID: partitionID,
			Offset:      offset,
		},
	}

	return msg, nil
}

func (s simpleGenerator) Name() string {
	return "simple"
}
