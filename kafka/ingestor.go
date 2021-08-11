package kafka

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"log"
	"time"
)

// For ingested rows, timestamp starts at this value and increments by one second for each row
var timestampBase = time.Date(2021, time.Month(4), 12, 9, 0, 0, 0, time.UTC)

// IngestRows ingests rows given schema and source name - convenience method for use in tests
func IngestRows(f *FakeKafka, sourceInfo *common.SourceInfo, rows *common.Rows, groupID string, encoder MessageEncoder) error {
	log.Println("Ingesting rows - getting topics and initial counts")
	topicName := sourceInfo.TopicInfo.TopicName
	topic, ok := f.GetTopic(topicName)
	if !ok {
		return fmt.Errorf("cannot find topic %s", topicName)
	}
	ingestedStart, _ := topic.TotalMessages(groupID)
	log.Println("ingesting the rows")
	timestamp := timestampBase
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		// We give each
		if err := IngestRow(f, topic, &row, sourceInfo.ColumnTypes, sourceInfo.PrimaryKeyCols, encoder, timestamp); err != nil {
			return err
		}
		timestamp = timestamp.Add(1 * time.Second)
	}
	log.Println("ingested all rows")
	// And we wait for all offsets to be committed
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		ingested, committed := topic.TotalMessages(groupID)
		// All the messages have been ingested and committed
		//log.Printf("start committed %d ingested %d committed %d ingested %d", c, ingestedStart, committed, ingested)
		if (ingested-ingestedStart == rows.RowCount()) && (ingested-committed) == 0 {
			return true, nil
		}
		return false, nil
	}, 10*time.Second, 50*time.Millisecond)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("messages not committed within timeout")
	}
	log.Println("waited for them to be committed")
	return nil
}

// IngestRow is a convenience method which encodes the row into a Kafka message first, then ingests it
func IngestRow(f *FakeKafka, topic *Topic, row *common.Row, colTypes []common.ColumnType, keyCols []int,
	encoder MessageEncoder, timestamp time.Time) error {
	message, err := encoder.EncodeMessage(row, colTypes, keyCols, timestamp)
	if err != nil {
		return err
	}
	err = topic.push(message)
	return err
}

func getColVal(colIndex int, colType common.ColumnType, row *common.Row) (interface{}, error) {
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
		ts := row.GetTimestamp(colIndex)
		gotime, err := ts.GoTime(time.UTC)
		if err != nil {
			return nil, err
		}
		// convert to unix millis past epoch
		colVal = gotime.UnixNano() / 1000000
	case common.TypeUnknown:
		panic("unknown type")
	}
	return colVal, nil
}
