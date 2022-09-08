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

func (s *simpleGenerator) Init() {
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

func (s *simpleGenerator) Name() string {
	return "simple"
}

type paymentsGenerator struct {
	uniqueIDsPerPartition int64
	paymentTypes          []string
	currencies            []string
}

func (p *paymentsGenerator) Init() {
	p.paymentTypes = []string{"btc", "p2p", "other"}
	p.currencies = []string{"gbp", "usd", "eur", "aud"}
}

func (p *paymentsGenerator) GenerateMessage(partitionID int32, offset int64, rnd *rand.Rand) (*kafka.Message, error) {
	m := make(map[string]interface{})
	// Payment id must be globally unique - so we include partition id and offset in it
	paymentID := fmt.Sprintf("payment-%010d-%019d", partitionID, offset)
	customerID := fmt.Sprintf("customer-token-%010d-%019d", partitionID, offset%p.uniqueIDsPerPartition)
	m["customer_token"] = customerID
	m["amount"] = fmt.Sprintf("%.2f", float64(rnd.Int31n(1000000))/10)
	m["payment_type"] = p.paymentTypes[int(offset)%len(p.paymentTypes)]
	m["currency"] = p.currencies[int(offset)%len(p.currencies)]
	json, err := json2.Marshal(&m)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	msg := &kafka.Message{
		Key:       []byte(paymentID),
		Value:     json,
		TimeStamp: time.Now(),
		PartInfo: kafka.PartInfo{
			PartitionID: partitionID,
			Offset:      offset,
		},
	}
	return msg, nil
}

func (p *paymentsGenerator) Name() string {
	return "payments"
}
