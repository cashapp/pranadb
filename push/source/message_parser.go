package source

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/protolib"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"reflect"
)

var (
	jsonDecoder         = &JSONDecoder{}
	kafkaDecoderFloat   = newKafkaDecoder(common.KafkaEncodingFloat32BE)
	kafkaDecoderDouble  = newKafkaDecoder(common.KafkaEncodingFloat64BE)
	kafkaDecoderInteger = newKafkaDecoder(common.KafkaEncodingInt32BE)
	kafkaDecoderLong    = newKafkaDecoder(common.KafkaEncodingInt64BE)
	kafkaDecoderShort   = newKafkaDecoder(common.KafkaEncodingInt16BE)
	kafkaDecoderString  = newKafkaDecoder(common.KafkaEncodingStringBytes)
)

const (
	tinyIntMinVal = -128
	tinyIntMaxVal = 127
	IntMinVal     = -2147483648
	IntMaxVal     = 2147483647
)

type MessageParser struct {
	sourceInfo       *common.SourceInfo
	rowsFactory      *common.RowsFactory
	colEvals         []evaluable
	headerDecoder    Decoder
	keyDecoder       Decoder
	valueDecoder     Decoder
	evalContext      *evalContext
	protobufRegistry protolib.Resolver
}

func NewMessageParser(sourceInfo *common.SourceInfo, registry protolib.Resolver) (*MessageParser, error) {
	selectors := sourceInfo.TopicInfo.ColSelectors
	selectEvals := make([]evaluable, len(selectors))
	// We pre-compute whether the selectors need headers, key and value so we don't unnecessary parse them if they
	// don't use them
	var (
		decodeHeader, decodeKey, decodeValue    bool
		headerDecoder, keyDecoder, valueDecoder Decoder

		err error
	)
	topic := sourceInfo.TopicInfo
	for i, selector := range selectors {
		selector := selector
		selectEvals[i] = selector.Select

		metaKey := selector.MetaKey
		if metaKey == nil {
			decodeValue = true
		} else {
			switch *metaKey {
			case "header":
				decodeHeader = true
			case "key":
				decodeKey = true
			case "timestamp":
				// timestamp selector, no decoding required
			default:
				panic(fmt.Sprintf("invalid selector %q", selector))
			}
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}

	mp := &MessageParser{
		rowsFactory:      common.NewRowsFactory(sourceInfo.ColumnTypes),
		protobufRegistry: registry,
		sourceInfo:       sourceInfo,
		colEvals:         selectEvals,
		headerDecoder:    headerDecoder,
		keyDecoder:       keyDecoder,
		valueDecoder:     valueDecoder,
		evalContext: &evalContext{
			meta: make(map[string]interface{}, 3),
		},
	}
	if decodeHeader {
		mp.headerDecoder, err = getDecoder(registry, topic.HeaderEncoding)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if decodeKey {
		mp.keyDecoder, err = getDecoder(registry, topic.KeyEncoding)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if decodeValue {
		mp.valueDecoder, err = getDecoder(registry, topic.ValueEncoding)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return mp, nil
}

func (m *MessageParser) ParseMessages(messages []*kafka.Message) (*common.Rows, error) {
	rows := m.rowsFactory.NewRows(len(messages))
	for _, msg := range messages {
		if err := m.decodeMessage(msg); err != nil {
			return nil, errors.WithStack(err)
		}
		if err := m.evalColumns(rows); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return rows, nil
}

func (m *MessageParser) decodeMessage(message *kafka.Message) error {
	// Decode headers
	var hdrs map[string]interface{}
	if m.headerDecoder != nil {
		lh := len(message.Headers)
		if lh > 0 {
			hdrs = make(map[string]interface{}, lh)
			for _, hdr := range message.Headers {
				hm, err := m.decodeBytes(m.headerDecoder, hdr.Value)
				if err != nil {
					return errors.WithStack(err)
				}
				hdrs[hdr.Key] = hm
			}
		}
	}
	// Decode key
	var km interface{}
	if m.keyDecoder != nil {
		var err error
		km, err = m.decodeBytes(m.keyDecoder, message.Key)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	// Decode value
	var vm interface{}
	if m.valueDecoder != nil {
		var err error
		vm, err = m.decodeBytes(m.valueDecoder, message.Value)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	m.evalContext.meta["header"] = hdrs
	m.evalContext.meta["key"] = km
	m.evalContext.meta["timestamp"] = message.TimeStamp
	m.evalContext.value = vm

	return nil
}

func checkIntBounds(val int64, min int64, max int64, typeName string) error {
	if val < min || val > max {
		return errors.NewValueOutOfRangeError(fmt.Sprintf("%s value %d is of out or range %d to %d", typeName, val, min, max))
	}
	return nil
}

func (m *MessageParser) evalColumns(rows *common.Rows) error { //nolint:gocyclo
	for i, eval := range m.colEvals {
		colType := m.sourceInfo.ColumnTypes[i]
		val, err := eval(m.evalContext.meta, m.evalContext.value)
		if err != nil {
			return errors.WithStack(err)
		}
		if val == nil {
			rows.AppendNullToColumn(i)
			continue
		}
		switch colType.Type {
		case common.TypeTinyInt:
			ival, err := CoerceInt64(val)
			if err != nil {
				return errors.WithStack(err)
			}
			if err := checkIntBounds(ival, tinyIntMinVal, tinyIntMaxVal, "TINYINT"); err != nil {
				return err
			}
			rows.AppendInt64ToColumn(i, ival)
		case common.TypeInt:
			ival, err := CoerceInt64(val)
			if err != nil {
				return errors.WithStack(err)
			}
			if err := checkIntBounds(ival, IntMinVal, IntMaxVal, "INT"); err != nil {
				return err
			}
			rows.AppendInt64ToColumn(i, ival)
		case common.TypeBigInt:
			ival, err := CoerceInt64(val)
			if err != nil {
				return errors.WithStack(err)
			}
			rows.AppendInt64ToColumn(i, ival)
		case common.TypeDouble:
			fval, err := CoerceFloat64(val)
			if err != nil {
				return errors.WithStack(err)
			}
			rows.AppendFloat64ToColumn(i, fval)
		case common.TypeVarchar:
			sval, err := CoerceString(val)
			if err != nil {
				return errors.WithStack(err)
			}
			rows.AppendStringToColumn(i, sval)
		case common.TypeDecimal:
			dval, err := CoerceDecimal(val)
			if err != nil {
				return errors.WithStack(err)
			}
			rows.AppendDecimalToColumn(i, *dval)
		case common.TypeTimestamp:
			tsVal, err := CoerceTimestamp(val)
			if err != nil {
				return errors.WithStack(err)
			}
			tsVal.SetFsp(colType.FSP)
			if err := common.RoundTimestampToFSP(&tsVal, colType.FSP); err != nil {
				return err
			}
			rows.AppendTimestampToColumn(i, tsVal)
		default:
			return errors.Errorf("unsupported col type %d", colType.Type)
		}
	}
	return nil
}

func getDecoder(registry protolib.Resolver, encoding common.KafkaEncoding) (Decoder, error) {
	var decoder Decoder
	switch encoding.Encoding {
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
		desc, err := registry.FindDescriptorByName(protoreflect.FullName(encoding.SchemaName))
		if err != nil {
			return nil, errors.Errorf("could not find protobuf descriptor for %q", encoding.SchemaName)
		}
		msgDesc, ok := desc.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, errors.Errorf("expected to find MessageDescriptor at %q, but was %q", encoding.SchemaName, reflect.TypeOf(msgDesc))
		}
		decoder = &ProtobufDecoder{desc: msgDesc}
	default:
		panic(fmt.Sprintf("unsupported encoding %+v", encoding))
	}
	return decoder, nil
}

func (m *MessageParser) decodeBytes(decoder Decoder, bytes []byte) (interface{}, error) {
	if bytes == nil {
		return nil, nil
	}
	v, err := decoder.Decode(bytes)
	return v, errors.WithStack(err)
}

type Decoder interface {
	Decode(bytes []byte) (interface{}, error)
}

type JSONDecoder struct {
}

func (j *JSONDecoder) Decode(bytes []byte) (interface{}, error) {
	m := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &m); err != nil {
		return nil, errors.WithStack(err)
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
	switch k.encoding.Encoding {
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
	desc protoreflect.MessageDescriptor
}

func (p *ProtobufDecoder) Decode(bytes []byte) (interface{}, error) {
	msg := dynamicpb.NewMessage(p.desc)
	err := proto.Unmarshal(bytes, msg)
	return msg, errors.WithStack(err)
}

type evaluable func(meta map[string]interface{}, v interface{}) (interface{}, error)
type evalContext struct {
	meta  map[string]interface{}
	value interface{}
}
