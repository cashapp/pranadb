package verifier

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"

	"github.com/google/go-cmp/cmp"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/msggen"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"google.golang.org/grpc"
)

type Verifier struct {
	gen msggen.MessageGenerator
}

func NewVerifier(genName string) (*Verifier, error) {
	return &Verifier{gen: &msggen.PaymentGenerator{}}, nil
}

func (v *Verifier) VerifyMessages(numMessages int64, indexStart int64, randSrc int64) error {
	rnd := rand.New(rand.NewSource(randSrc))
	gmsgs := make(map[string]map[string]interface{}, numMessages)
	for i := indexStart; i < indexStart+numMessages; i++ {
		gmsg, err := v.gen.GenerateMessage2(i, rnd)
		if err != nil {
			return errors.WithStack(err)
		}
		m := gmsg.JsonFields
		m["payment_time"] = gmsg.Timestamp.UnixMicro()
		m["fraud_score"], err = strconv.ParseFloat(string(gmsg.KafkaHeaders[0].Value), 64)
		if err != nil {
			return err
		}
		gmsgs[gmsg.Key] = m
	}
	serverAddress := "localhost:6584"
	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { conn.Close() }()
	client := service.NewPranaDBServiceClient(conn)
	if err != nil {
		return errors.WithStack(err)
	}
	c, err := client.ExecuteSQLStatement(context.Background(), &service.ExecuteSQLStatementRequest{
		Schema:    "test",
		Statement: "select * from payments",
		PageSize:  3,
	})
	col, err := c.Recv()
	if err != nil {
		return errors.WithStack(err)
	}
	receivedMap := make(map[string]map[string]interface{}, 0)
	columns := col.GetColumns().Columns
	for i := 0; ; i++ {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		page := resp.GetPage()
		for _, row := range page.GetRows() {
			entry := make(map[string]interface{}, len(columns))
			for i, c := range columns {
				switch c.GetType() {
				case service.ColumnType_COLUMN_TYPE_INT, service.ColumnType_COLUMN_TYPE_BIG_INT, service.ColumnType_COLUMN_TYPE_TINY_INT:
					entry[c.Name] = row.GetValues()[i].GetIntValue()
				case service.ColumnType_COLUMN_TYPE_DOUBLE:
					entry[c.Name] = row.GetValues()[i].GetFloatValue()
				case service.ColumnType_COLUMN_TYPE_DECIMAL:
					entry[c.Name] = row.GetValues()[i].GetStringValue()
				case service.ColumnType_COLUMN_TYPE_VARCHAR:
					entry[c.Name] = row.GetValues()[i].GetStringValue()
				case service.ColumnType_COLUMN_TYPE_TIMESTAMP:
					entry[c.Name] = row.GetValues()[i].GetIntValue()
				}
			}
			paymentId := (entry["payment_id"]).(string)
			delete(entry, "payment_id")
			receivedMap[paymentId] = entry
		}
	}
	if !cmp.Equal(gmsgs, receivedMap) {
		fmt.Fprintln(os.Stderr, "verification failed")
		fmt.Fprintln(os.Stderr, cmp.Diff(gmsgs, receivedMap))
	}

	return nil
}
