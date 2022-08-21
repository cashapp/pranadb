package http

import (
	"encoding/json"
	"fmt"
	"github.com/squareup/pranadb/common"
	"net/http"
	"time"
)

// Each row is written as a JSON array separated by new line
type jsonLinesRowWriter struct {
}

func (j *jsonLinesRowWriter) WriteContentType(writer http.ResponseWriter) {
	// We use text/plain as it allows browsers to display it easily
	writer.Header().Add("Content-Type", "text/plain")
}

func (j *jsonLinesRowWriter) WriteRow(row *common.Row, writer http.ResponseWriter) error {
	colCount := row.ColCount()
	arr := make([]interface{}, colCount)
	for i, colType := range row.ColumnTypes() {
		var val interface{}
		if row.IsNull(i) {
			val = nil
		} else {
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val = row.GetInt64(i)
			case common.TypeDouble:
				val = row.GetFloat64(i)
			case common.TypeVarchar:
				val = row.GetString(i)
			case common.TypeDecimal:
				dec := row.GetDecimal(i)
				// We encode the decimal as a string
				val = dec.String()
			case common.TypeTimestamp:
				ts := row.GetTimestamp(i)
				gt, err := ts.GoTime(time.UTC)
				if err != nil {
					return err
				}
				// We encode a datetime as *microseconds* past epoch
				val = gt.UnixNano() / 1000
			default:
				panic(fmt.Sprintf("unexpected column type %d", colType.Type))
			}
		}
		arr[i] = val
	}
	bytes, err := json.Marshal(arr)
	if err != nil {
		return err
	}
	if _, err := writer.Write(bytes); err != nil {
		return err
	}
	if _, err := writer.Write(newLine); err != nil {
		return err
	}
	return nil
}

var newLine = []byte{'\n'}
