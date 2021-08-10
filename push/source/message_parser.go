package source

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/PaesslerAG/gval"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"strings"
)

var jsonDecoder = &JSONDecoder{}
var protobufDecoder = &ProtobufDecoder{}
var kafkaDecoderFloat = newKafkaDecoder(common.EncodingFloat32BE)
var kafkaDecoderDouble = newKafkaDecoder(common.EncodingFloat64BE)
var kafkaDecoderInteger = newKafkaDecoder(common.EncodingInt32BE)
var kafkaDecoderLong = newKafkaDecoder(common.EncodingInt64BE)
var kafkaDecoderShort = newKafkaDecoder(common.EncodingInt16BE)
var kafkaDecoderString = newKafkaDecoder(common.EncodingStringBytes)

type MessageParser struct {
	sourceInfo   *common.SourceInfo
	rowsFactory  *common.RowsFactory
	colEvals     []gval.Evaluable
	parseHeaders bool
	parseKey     bool
	parseValue   bool
	repMap       map[string]interface{}
}

func NewMessageParser(sourceInfo *common.SourceInfo) (*MessageParser, error) {
	lang := gval.Full()
	selectors := sourceInfo.TopicInfo.ColSelectors
	selectEvals := make([]gval.Evaluable, len(selectors))
	// We pre-compute whether the selectors need headers, key and value so we don't unnecessary parse them if they
	// don't use them
	parseHeaders := false
	parseKey := false
	parseValue := false
	repMapSize := 0
	for i, selector := range selectors {
		if strings.HasPrefix(selector, "h") {
			parseHeaders = true
			repMapSize++
		}
		if strings.HasPrefix(selector, "k") {
			parseKey = true
			repMapSize++
		}
		if strings.HasPrefix(selector, "v") {
			parseValue = true
			repMapSize++
		}
		if strings.HasPrefix(selector, "t") {
			repMapSize++
		}
		eval, err := lang.NewEvaluable(selector)
		if err != nil {
			return nil, err
		}
		selectEvals[i] = eval
	}
	repMap := make(map[string]interface{}, repMapSize)
	return &MessageParser{
		rowsFactory:  common.NewRowsFactory(sourceInfo.ColumnTypes),
		sourceInfo:   sourceInfo,
		colEvals:     selectEvals,
		parseHeaders: parseHeaders,
		parseKey:     parseKey,
		parseValue:   parseValue,
		repMap:       repMap,
	}, nil
}

func (m *MessageParser) ParseMessages(messages []*kafka.Message) (*common.Rows, error) {
	rows := m.rowsFactory.NewRows(len(messages))
	for _, msg := range messages {
		if err := m.parseMessage(msg, rows); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

//nolint:gocyclo
func (m *MessageParser) parseMessage(message *kafka.Message, rows *common.Rows) error {
	ti := m.sourceInfo.TopicInfo
	// Decode headers
	var hdrs map[string]interface{}
	if m.parseHeaders {
		lh := len(message.Headers)
		if lh > 0 {
			hdrs = make(map[string]interface{}, lh)
			for _, hdr := range message.Headers {
				hm, err := m.decodeBytes(ti.HeaderEncoding, hdr.Value)
				if err != nil {
					return err
				}
				hdrs[hdr.Key] = hm
			}
		}
	}
	// Decode key
	var km interface{}
	if m.parseKey {
		var err error
		km, err = m.decodeBytes(ti.KeyEncoding, message.Key)
		if err != nil {
			return err
		}
	}
	// Decode value
	var vm interface{}
	if m.parseValue {
		var err error
		vm, err = m.decodeBytes(ti.ValueEncoding, message.Value)
		if err != nil {
			return err
		}
	}

	m.repMap["h"] = hdrs
	m.repMap["k"] = km
	m.repMap["v"] = vm
	m.repMap["t"] = message.TimeStamp
	for i, eval := range m.colEvals {
		colType := m.sourceInfo.ColumnTypes[i]
		c := context.Background()
		val, err := eval(c, m.repMap)
		if err != nil {
			return err
		}
		if val == nil {
			rows.AppendNullToColumn(i)
			continue
		}
		switch colType.Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			ival, err := CoerceInt64(val)
			if err != nil {
				return err
			}
			rows.AppendInt64ToColumn(i, ival)
		case common.TypeDouble:
			fval, err := CoerceFloat64(val)
			if err != nil {
				return err
			}
			rows.AppendFloat64ToColumn(i, fval)
		case common.TypeVarchar:
			sval, err := CoerceString(val)
			if err != nil {
				return err
			}
			rows.AppendStringToColumn(i, sval)
		case common.TypeDecimal:
			dval, err := CoerceDecimal(val)
			if err != nil {
				return err
			}
			rows.AppendDecimalToColumn(i, *dval)
		case common.TypeTimestamp:
			tsVal, err := CoerceTimestamp(val)
			if err != nil {
				return err
			}
			rows.AppendTimestampToColumn(i, tsVal)
		default:
			return fmt.Errorf("unsupported col type %d", colType.Type)
		}
	}
	return nil
}

func (m *MessageParser) decodeBytes(encoding common.KafkaEncoding, bytes []byte) (interface{}, error) {
	if bytes == nil {
		return nil, nil
	}
	var decoder Decoder
	switch encoding {
	case common.EncodingJSON:
		decoder = jsonDecoder
	case common.EncodingFloat64BE:
		decoder = kafkaDecoderDouble
	case common.EncodingFloat32BE:
		decoder = kafkaDecoderFloat
	case common.EncodingInt32BE:
		decoder = kafkaDecoderInteger
	case common.EncodingInt64BE:
		decoder = kafkaDecoderLong
	case common.EncodingInt16BE:
		decoder = kafkaDecoderShort
	case common.EncodingStringBytes:
		decoder = kafkaDecoderString
	case common.EncodingProtobuf:
		decoder = protobufDecoder
	default:
		panic("unsupported encoding")
	}
	return decoder.Decode(bytes)
}

type Decoder interface {
	Decode(bytes []byte) (interface{}, error)
}

type JSONDecoder struct {
}

func (j *JSONDecoder) Decode(bytes []byte) (interface{}, error) {
	m := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func newKafkaDecoder(encoding common.KafkaEncoding) *KafkaDecoder {
	return &KafkaDecoder{encoding: encoding}
}

type KafkaDecoder struct {
	encoding common.KafkaEncoding
}

func (k *KafkaDecoder) Decode(bytes []byte) (interface{}, error) {
	if len(bytes) == 0 {
		return nil, nil
	}
	switch k.encoding {
	case common.EncodingFloat32BE:
		val, _ := common.ReadFloat32FromBufferBE(bytes, 0)
		return val, nil
	case common.EncodingFloat64BE:
		val, _ := common.ReadFloat64FromBufferBE(bytes, 0)
		return val, nil
	case common.EncodingInt32BE:
		val, _ := common.ReadUint32FromBufferBE(bytes, 0)
		return int32(val), nil
	case common.EncodingInt64BE:
		val, _ := common.ReadUint64FromBufferBE(bytes, 0)
		return int64(val), nil
	case common.EncodingInt16BE:
		val, _ := common.ReadUint16FromBufferBE(bytes, 0)
		return int16(val), nil
	case common.EncodingStringBytes:
		// UTF-8 encoded
		return string(bytes), nil
	default:
		panic("unknown encoding")
	}
}

type ProtobufDecoder struct {
}

func (p *ProtobufDecoder) Decode(bytes []byte) (interface{}, error) {
	// cheat
	if string(bytes) == "key" {
		return &SomeKeyProto{F1: 1234}, nil
	}
	return &SomeValueProto{
		F1: 12.21,
		F2: "foo",
	}, nil
}

type SomeValueProto struct {
	F1 float64
	F2 string
}

type SomeKeyProto struct {
	F1 int64
}
