package codec

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protolib"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"math"
	"reflect"
)

var (
	jsonCodec         = &JSONCodec{}
	kafkaCodecFloat   = newKafkaCodec(common.KafkaEncodingFloat32BE)
	kafkaCodecDouble  = newKafkaCodec(common.KafkaEncodingFloat64BE)
	kafkaCodecInteger = newKafkaCodec(common.KafkaEncodingInt32BE)
	kafkaCodecLong    = newKafkaCodec(common.KafkaEncodingInt64BE)
	kafkaCodecShort   = newKafkaCodec(common.KafkaEncodingInt16BE)
	kafkaCodecString  = newKafkaCodec(common.KafkaEncodingStringBytes)
)

func GetCodec(registry protolib.Resolver, encoding common.KafkaEncoding) (Codec, error) {
	var decoder Codec
	switch encoding.Encoding {
	case common.EncodingJSON:
		decoder = jsonCodec
	case common.EncodingFloat64BE:
		decoder = kafkaCodecDouble
	case common.EncodingFloat32BE:
		decoder = kafkaCodecFloat
	case common.EncodingInt32BE:
		decoder = kafkaCodecInteger
	case common.EncodingInt64BE:
		decoder = kafkaCodecLong
	case common.EncodingInt16BE:
		decoder = kafkaCodecShort
	case common.EncodingStringBytes:
		decoder = kafkaCodecString
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

type Codec interface {
	Decode(bytes []byte) (interface{}, error)
	Encode(val interface{}) ([]byte, error)
}

type JSONCodec struct {
}

func (j *JSONCodec) Decode(bytes []byte) (interface{}, error) {
	m := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &m); err != nil {
		return nil, errors.WithStack(err)
	}
	return m, nil
}

func (j *JSONCodec) Encode(val interface{}) ([]byte, error) {
	return json.Marshal(val)
}

func newKafkaCodec(encoding common.KafkaEncoding) *KafkaCodec {
	return &KafkaCodec{encoding: encoding}
}

type KafkaCodec struct {
	encoding common.KafkaEncoding
}

func (k *KafkaCodec) Decode(bytes []byte) (interface{}, error) {
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

func (k *KafkaCodec) Encode(val interface{}) ([]byte, error) {
	switch k.encoding.Encoding {
	case common.EncodingFloat32BE:
		f, err := CoerceFloat64(val)
		if err != nil {
			return nil, err
		}
		if f > math.MaxFloat32 {
			return nil, errors.Errorf("value %f is too large to be converted to float32", f)
		}
		buff := make([]byte, 0, 4)
		buff = common.AppendFloat32ToBufferBE(buff, float32(f))
		return buff, nil
	case common.EncodingFloat64BE:
		f, err := CoerceFloat64(val)
		if err != nil {
			return nil, err
		}
		buff := make([]byte, 0, 8)
		buff = common.AppendFloat64ToBufferBE(buff, f)
		return buff, nil
	case common.EncodingInt32BE:
		i, err := CoerceInt64(val)
		if err != nil {
			return nil, err
		}
		if i > math.MaxInt32 {
			return nil, errors.Errorf("value %d is too large to be converted to int32", i)
		}
		i32 := int32(i)
		buff := make([]byte, 0, 4)
		buff = common.AppendUint32ToBufferBE(buff, uint32(i32))
		return buff, nil
	case common.EncodingInt64BE:
		i, err := CoerceInt64(val)
		if err != nil {
			return nil, err
		}
		buff := make([]byte, 0, 8)
		buff = common.AppendUint64ToBufferBE(buff, uint64(i))
		return buff, nil
	case common.EncodingInt16BE:
		i, err := CoerceInt64(val)
		if err != nil {
			return nil, err
		}
		if i > math.MaxInt16 {
			return nil, errors.Errorf("value %d is too large to be converted to int16", i)
		}
		i16 := int16(i)
		v := uint16(i16)
		buff := make([]byte, 0, 2)
		buff = append(buff, byte(v>>8), byte(v))
		return buff, nil
	case common.EncodingStringBytes:
		v, err := CoerceString(val)
		if err != nil {
			return nil, err
		}
		// UTF-8 encoded
		return []byte(v), nil
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

func (p *ProtobufDecoder) Encode(val interface{}) ([]byte, error) {
	// TODO!!!!!
	msg := dynamicpb.NewMessage(p.desc)
	return proto.Marshal(msg)
}
