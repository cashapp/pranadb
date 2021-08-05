package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"log"
	"time"
)

// IngestRows ingests rows given schema and source name - convenience method for use in tests
func IngestRows(f *FakeKafka, sourceInfo *common.SourceInfo, rows *common.Rows,
	keyEncoding common.KafkaEncoding, valueEncoding common.KafkaEncoding, groupID string) error {
	topicName := sourceInfo.TopicInfo.TopicName

	topic, ok := f.GetTopic(topicName)
	if !ok {
		return fmt.Errorf("cannot find topic %s", topicName)
	}
	startCommitted := topic.TotalCommittedMessages(groupID)

	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		if err := IngestRow(f, topicName, &row, sourceInfo.ColumnTypes, sourceInfo.PrimaryKeyCols, keyEncoding, valueEncoding); err != nil {
			return err
		}
	}
	// And we wait for all offsets to be committed
	wanted := startCommitted + rows.RowCount()
	log.Printf("Want %d  committed messages", wanted)
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		n := topic.TotalCommittedMessages(groupID)
		log.Printf("Got %d committed messages", n)
		return n == wanted, nil
	}, 5*time.Second, 10*time.Millisecond)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("messages not committed within timeout")
	}
	return nil
}

// IngestRow is a convenience method which encodes the row into a Kafka message first, then ingests it
func IngestRow(f *FakeKafka, topicName string, row *common.Row, colTypes []common.ColumnType, keyCols []int,
	keyEncoding common.KafkaEncoding, valueEncoding common.KafkaEncoding) error {
	topic, ok := f.getTopic(topicName)
	if !ok {
		return fmt.Errorf("no such topic %s", topicName)
	}

	// TODO support other encodings so we can test them from SQLTest
	if keyEncoding != common.EncodingJSON || valueEncoding != common.EncodingJSON {
		return errors.New("only JSON key and value encodings for ingesting currently supported")
	}

	keyMap := map[string]interface{}{}
	for i, keyCol := range keyCols {
		colType := colTypes[keyCol]
		colVal := getColVal(keyCol, colType, row)
		keyMap[fmt.Sprintf("k%d", i)] = colVal
	}

	valMap := map[string]interface{}{}
	for i, colType := range colTypes {
		colVal := getColVal(i, colType, row)
		valMap[fmt.Sprintf("v%d", i)] = colVal
	}

	keyBytes, err := json.Marshal(keyMap)
	if err != nil {
		return err
	}

	valBytes, err := json.Marshal(valMap)
	if err != nil {
		return err
	}

	message := &Message{
		Key:   keyBytes,
		Value: valBytes,
	}
	topic.push(message)
	return nil
}

func getColVal(colIndex int, colType common.ColumnType, row *common.Row) interface{} {
	var colVal interface{}
	switch colType.Type {
	case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
		colVal = row.GetInt64(colIndex)
	case common.TypeDouble:
		colVal = row.GetFloat64(colIndex)
	case common.TypeVarchar:
		colVal = row.GetString(colIndex)
	case common.TypeDecimal:
		dec := row.GetDecimal(colIndex)
		colVal = dec.String()
	case common.TypeTimestamp:
		panic("TODO")
	case common.TypeUnknown:
		panic("unknown type")
	}
	return colVal
}
