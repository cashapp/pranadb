package source

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/PaesslerAG/gval"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
)

var jsonDecoder = &JSONDecoder{}
var protobufDecoder = &ProtobufDecoder{}
var kafkaDecoderFloat = newKafkaDecoder(common.EncodingKafkaFloat)
var kafkaDecoderDouble = newKafkaDecoder(common.EncodingKafkaDouble)
var kafkaDecoderInteger = newKafkaDecoder(common.EncodingKafkaInteger)
var kafkaDecoderLong = newKafkaDecoder(common.EncodingKafkaLong)
var kafkaDecoderShort = newKafkaDecoder(common.EncodingKafkaShort)
var kafkaDecoderString = newKafkaDecoder(common.EncodingKafkaString)

type MessageParser struct {
	sourceInfo  *common.SourceInfo
	rowsFactory *common.RowsFactory
	colEvals    []gval.Evaluable
}

func NewMessageParser(sourceInfo *common.SourceInfo) (*MessageParser, error) {
	lang := gval.Full()
	selectors := sourceInfo.TopicInfo.ColSelectors
	selectEvals := make([]gval.Evaluable, len(selectors))
	for i, selector := range selectors {
		eval, err := lang.NewEvaluable(selector)
		if err != nil {
			return nil, err
		}
		selectEvals[i] = eval
	}
	return &MessageParser{
		rowsFactory: common.NewRowsFactory(sourceInfo.ColumnTypes),
		sourceInfo:  sourceInfo,
		colEvals:    selectEvals,
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

func (m *MessageParser) parseMessage(message *kafka.Message, rows *common.Rows) error {
	ti := m.sourceInfo.TopicInfo
	// Decode headers
	var hdrs map[string]interface{}
	if lh := len(message.Headers); lh > 0 {
		hdrs = make(map[string]interface{}, lh)
		for _, hdr := range message.Headers {
			hm, err := m.decodeBytes(ti.HeaderEncoding, hdr.Value)
			if err != nil {
				return err
			}
			hdrs[hdr.Key] = hm
		}
	}
	// Decode key
	km, err := m.decodeBytes(ti.KeyEncoding, message.Key)
	if err != nil {
		return err
	}
	// Decode value
	vm, err := m.decodeBytes(ti.ValueEncoding, message.Value)
	if err != nil {
		return err
	}
	rep := make(map[string]interface{}, 2)
	rep["h"] = hdrs
	rep["k"] = km
	rep["v"] = vm
	rep["t"] = message.TimeStamp
	for i, eval := range m.colEvals {
		colType := m.sourceInfo.ColumnTypes[i]
		c := context.Background()
		val, err := eval(c, rep)
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
	case common.EncodingKafkaDouble:
		decoder = kafkaDecoderDouble
	case common.EncodingKafkaFloat:
		decoder = kafkaDecoderFloat
	case common.EncodingKafkaInteger:
		decoder = kafkaDecoderInteger
	case common.EncodingKafkaLong:
		decoder = kafkaDecoderLong
	case common.EncodingKafkaShort:
		decoder = kafkaDecoderShort
	case common.EncodingKafkaString:
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
	case common.EncodingKafkaFloat:
		val, _ := common.ReadFloat32FromBufferBE(bytes, 0)
		return val, nil
	case common.EncodingKafkaDouble:
		val, _ := common.ReadFloat64FromBufferBE(bytes, 0)
		return val, nil
	case common.EncodingKafkaInteger:
		val, _ := common.ReadUint32FromBufferBE(bytes, 0)
		return int32(val), nil
	case common.EncodingKafkaLong:
		val, _ := common.ReadUint64FromBufferBE(bytes, 0)
		return int64(val), nil
	case common.EncodingKafkaShort:
		val, _ := common.ReadUint16FromBufferBE(bytes, 0)
		return int16(val), nil
	case common.EncodingKafkaString:
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
