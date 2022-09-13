package msggen

import (
	json2 "encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/kafka"
)

// Example message generators

type PaymentGenerator struct {
}

func (p *PaymentGenerator) Name() string {
	return "payments"
}

func (p *PaymentGenerator) Init() {
}

func (p *PaymentGenerator) GenerateMessage(_ int32, index int64, rnd *rand.Rand) (*kafka.Message, error) {

	paymentTypes := []string{"btc", "p2p", "other"}
	currencies := []string{"gbp", "usd", "eur", "aud"}
	timestamp := time.Now()

	m := make(map[string]interface{})
	paymentID := fmt.Sprintf("payment%06d", index)
	customerID := index % 17
	m["customer_id"] = customerID
	m["amount"] = fmt.Sprintf("%.2f", float64(rnd.Int31n(1000000))/10)
	m["payment_type"] = paymentTypes[int(index)%len(paymentTypes)]
	m["currency"] = currencies[int(index)%len(currencies)]
	json, err := json2.Marshal(&m)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var headers []kafka.MessageHeader
	fs := rnd.Float64()
	headers = append(headers, kafka.MessageHeader{
		Key:   "fraud_score",
		Value: []byte(fmt.Sprintf("%.2f", fs)),
	})

	msg := &kafka.Message{
		Key:       []byte(paymentID),
		Value:     json,
		TimeStamp: timestamp,
		Headers:   headers,
	}

	return msg, nil
}
