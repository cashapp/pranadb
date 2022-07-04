package kafka

import (
	"fmt"
	"time"

	"github.com/squareup/pranadb/errors"

	"github.com/squareup/pranadb/common"
)

// For ingested rows, timestamp starts at this value and increments by one second for each row
var timestampBase = time.Date(2021, time.Month(4), 12, 9, 0, 0, 0, time.UTC)

// IngestRows ingests rows given schema and source name - convenience method for use in tests
func IngestRows(f *FakeKafka, sourceInfo *common.SourceInfo, colTypes []common.ColumnType, rows *common.Rows, encoder MessageEncoder) error {
	topicName := sourceInfo.OriginInfo.TopicName
	topic, ok := f.GetTopic(topicName)
	if !ok {
		return errors.Errorf("cannot find topic %s", topicName)
	}
	timestamp := timestampBase
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		// We give each
		if err := IngestRow(topic, &row, colTypes, sourceInfo.PrimaryKeyCols, encoder, timestamp); err != nil {
			return errors.WithStack(err)
		}
		timestamp = timestamp.Add(1 * time.Microsecond)
	}
	return nil
}

// IngestRow is a convenience method which encodes the row into a Kafka message first, then ingests it
func IngestRow(topic *Topic, row *common.Row, colTypes []common.ColumnType, keyCols []int, encoder MessageEncoder, timestamp time.Time) error {
	message, err := encoder.EncodeMessage(row, colTypes, keyCols, timestamp)
	if err != nil {
		return errors.WithStack(err)
	}
	err = topic.push(message)
	return errors.WithStack(err)
}

func getColVal(colIndex int, colType common.ColumnType, row *common.Row) interface{} {
	if row.IsNull(colIndex) {
		return nil
	}
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
		// Get as ISO-8601 string
		ct := ts.CoreTime()
		colVal = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%06d", ct.Year(), ct.Month(), ct.Day(), ct.Hour(), ct.Minute(), ct.Second(), ct.Microsecond())
	case common.TypeUnknown:
		panic("unknown type")
	}
	return colVal
}
