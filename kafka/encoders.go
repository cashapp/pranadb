package kafka

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protolib"
	pref "google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type MessageEncoder interface {
	Name() string
	EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error)
}

// JSONKeyJSONValueEncoder encodes as top level JSON key, top level JSON value, no headers
type JSONKeyJSONValueEncoder struct {
}

func (s *JSONKeyJSONValueEncoder) Name() string {
	return "JSONKeyJSONValueEncoder"
}

func (s *JSONKeyJSONValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	keyMap := map[string]interface{}{}
	for i, keyCol := range keyCols {
		colType := colTypes[keyCol]
		colVal, err := getColVal(keyCol, colType, row)
		if err != nil {
			return nil, err
		}
		keyMap[fmt.Sprintf("k%d", i)] = colVal
	}
	valMap := map[string]interface{}{}
	for i, colType := range colTypes {
		colVal, err := getColVal(i, colType, row)
		if err != nil {
			return nil, err
		}
		valMap[fmt.Sprintf("v%d", i)] = colVal
	}
	keyBytes, err := json.Marshal(keyMap)
	if err != nil {
		return nil, err
	}
	valBytes, err := json.Marshal(valMap)
	if err != nil {
		return nil, err
	}
	message := &Message{
		TimeStamp: timestamp,
		Key:       keyBytes,
		Value:     valBytes,
	}
	return message, nil
}

// StringKeyTLJSONValueEncoder encodes as string key, top level JSON value, no headers
type StringKeyTLJSONValueEncoder struct {
}

func (s *StringKeyTLJSONValueEncoder) Name() string {
	return "StringKeyTLJSONValueEncoder"
}

func (s *StringKeyTLJSONValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	if len(keyCols) != 1 {
		return nil, errors.New("must be only one pk col for binary key encoding")
	}
	keyColIndex := keyCols[0]
	keyColType := colTypes[keyColIndex]
	if keyColType != common.VarcharColumnType {
		return nil, errors.New("Key is not a varchar column")
	}
	keyBytes := []byte(row.GetString(keyColIndex))

	valBytes, err := encodeTLJSONValue(colTypes, row, keyColIndex)
	if err != nil {
		return nil, err
	}

	return &Message{
		Key:   keyBytes,
		Value: valBytes,
	}, nil
}

// Int64BEKeyTLJSONValueEncoder encodes as int64BE key, top level JSON value, no headers
type Int64BEKeyTLJSONValueEncoder struct {
}

func (s *Int64BEKeyTLJSONValueEncoder) Name() string {
	return "Int64BEKeyTLJSONValueEncoder"
}

func (s *Int64BEKeyTLJSONValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	if len(keyCols) != 1 {
		return nil, errors.New("must be only one pk col for binary key encoding")
	}
	keyColIndex := keyCols[0]
	keyColType := colTypes[keyColIndex]
	if keyColType != common.BigIntColumnType {
		return nil, errors.New("Key is not a BIGINT column")
	}
	keyBytes := common.AppendUint64ToBufferBE(nil, uint64(row.GetInt64(keyColIndex)))

	valBytes, err := encodeTLJSONValue(colTypes, row, keyColIndex)
	if err != nil {
		return nil, err
	}

	return &Message{
		Key:   keyBytes,
		Value: valBytes,
	}, nil
}

// Int32BEKeyTLJSONValueEncoder encodes as int32BE key, top level JSON value, no headers
type Int32BEKeyTLJSONValueEncoder struct {
}

func (s *Int32BEKeyTLJSONValueEncoder) Name() string {
	return "Int32BEKeyTLJSONValueEncoder"
}

func (s *Int32BEKeyTLJSONValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	if len(keyCols) != 1 {
		return nil, errors.New("must be only one pk col for binary key encoding")
	}
	keyColIndex := keyCols[0]
	keyColType := colTypes[keyColIndex]
	if keyColType != common.IntColumnType {
		return nil, errors.New("Key is not a INTEGER column")
	}
	keyBytes := common.AppendUint32ToBufferBE(nil, uint32(row.GetInt64(keyColIndex)))

	valBytes, err := encodeTLJSONValue(colTypes, row, keyColIndex)
	if err != nil {
		return nil, err
	}

	return &Message{
		Key:   keyBytes,
		Value: valBytes,
	}, nil
}

// Int16BEKeyTLJSONValueEncoder encodes as int16BE key, top level JSON value, no headers
type Int16BEKeyTLJSONValueEncoder struct {
}

func (s *Int16BEKeyTLJSONValueEncoder) Name() string {
	return "Int16BEKeyTLJSONValueEncoder"
}

func (s *Int16BEKeyTLJSONValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	if len(keyCols) != 1 {
		return nil, errors.New("must be only one pk col for binary key encoding")
	}
	keyColIndex := keyCols[0]
	keyColType := colTypes[keyColIndex]
	if keyColType != common.IntColumnType {
		return nil, errors.New("Key is not a INTEGER column")
	}
	keyBytes := common.AppendUint16ToBufferBE(nil, uint16(row.GetInt64(keyColIndex)))

	valBytes, err := encodeTLJSONValue(colTypes, row, keyColIndex)
	if err != nil {
		return nil, err
	}

	return &Message{
		Key:   keyBytes,
		Value: valBytes,
	}, nil
}

// Float64BEKeyTLJSONValueEncoder encodes as float64BE key, top level JSON value, no headers
type Float64BEKeyTLJSONValueEncoder struct {
}

func (s *Float64BEKeyTLJSONValueEncoder) Name() string {
	return "Float64BEKeyTLJSONValueEncoder"
}

func (s *Float64BEKeyTLJSONValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	if len(keyCols) != 1 {
		return nil, errors.New("must be only one pk col for binary key encoding")
	}
	keyColIndex := keyCols[0]
	keyColType := colTypes[keyColIndex]
	if keyColType != common.DoubleColumnType {
		return nil, errors.New("Key is not a DOUBLE column")
	}
	keyBytes := common.AppendFloat64ToBufferBE(nil, row.GetFloat64(keyColIndex))

	valBytes, err := encodeTLJSONValue(colTypes, row, keyColIndex)
	if err != nil {
		return nil, err
	}

	return &Message{
		Key:   keyBytes,
		Value: valBytes,
	}, nil
}

// Float32BEKeyTLJSONValueEncoder encodes as float32BE key, top level JSON value, no headers
type Float32BEKeyTLJSONValueEncoder struct {
}

func (s *Float32BEKeyTLJSONValueEncoder) Name() string {
	return "Float32BEKeyTLJSONValueEncoder"
}

func (s *Float32BEKeyTLJSONValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	if len(keyCols) != 1 {
		return nil, errors.New("must be only one pk col for binary key encoding")
	}
	keyColIndex := keyCols[0]
	keyColType := colTypes[keyColIndex]
	if keyColType != common.DoubleColumnType {
		return nil, errors.New("Key is not a DOUBLE column")
	}
	keyBytes := common.AppendFloat32ToBufferBE(nil, float32(row.GetFloat64(keyColIndex)))

	valBytes, err := encodeTLJSONValue(colTypes, row, keyColIndex)
	if err != nil {
		return nil, err
	}

	return &Message{
		Key:   keyBytes,
		Value: valBytes,
	}, nil
}

func encodeTLJSONValue(colTypes []common.ColumnType, row *common.Row, keyColIndex int) ([]byte, error) {
	valMap := map[string]interface{}{}
	for i, colType := range colTypes {
		if i == keyColIndex {
			continue
		}
		colVal, err := getColVal(i, colType, row)
		if err != nil {
			return nil, err
		}
		valMap[fmt.Sprintf("v%d", i)] = colVal
	}
	return json.Marshal(valMap)

}

// NestedJSONKeyNestedJSONValueEncoder encodes as nested JSON key, nested JSON value, no headers
type NestedJSONKeyNestedJSONValueEncoder struct {
}

func (s *NestedJSONKeyNestedJSONValueEncoder) Name() string {
	return "NestedJSONKeyNestedJSONValueEncoder"
}

func (s *NestedJSONKeyNestedJSONValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	keyMap := map[string]interface{}{}
	for i, keyCol := range keyCols {
		colType := colTypes[keyCol]
		colVal, err := getColVal(keyCol, colType, row)
		if err != nil {
			return nil, err
		}
		nested := map[string]interface{}{}
		keyMap[fmt.Sprintf("n%d", i)] = nested
		nested[fmt.Sprintf("k%d", i)] = colVal
	}
	valMap := map[string]interface{}{}
	for i, colType := range colTypes {
		colVal, err := getColVal(i, colType, row)
		if err != nil {
			return nil, err
		}
		nested := map[string]interface{}{}
		valMap[fmt.Sprintf("n%d", i)] = nested
		nested[fmt.Sprintf("v%d", i)] = colVal
	}
	keyBytes, err := json.Marshal(keyMap)
	if err != nil {
		return nil, err
	}
	valBytes, err := json.Marshal(valMap)
	if err != nil {
		return nil, err
	}
	message := &Message{
		Key:   keyBytes,
		Value: valBytes,
	}
	return message, nil
}

// JSONHeadersEncoder puts the message key encoded as JSON in one header and the message value encoded as JSON
// in another, the actual message key and value are empty JSON objects
type JSONHeadersEncoder struct {
}

func (s *JSONHeadersEncoder) Name() string {
	return "JSONHeadersEncoder"
}

func (s *JSONHeadersEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	keyHeaderMap := map[string]interface{}{}
	for i, keyCol := range keyCols {
		colType := colTypes[keyCol]
		colVal, err := getColVal(keyCol, colType, row)
		if err != nil {
			return nil, err
		}
		keyHeaderMap[fmt.Sprintf("k%d", i)] = colVal
	}
	valHeaderMap := map[string]interface{}{}
	for i, colType := range colTypes {
		colVal, err := getColVal(i, colType, row)
		if err != nil {
			return nil, err
		}
		valHeaderMap[fmt.Sprintf("v%d", i)] = colVal
	}
	keyHeaderBytes, err := json.Marshal(keyHeaderMap)
	if err != nil {
		return nil, err
	}
	valHeaderBytes, err := json.Marshal(valHeaderMap)
	if err != nil {
		return nil, err
	}

	keyBytes, err := json.Marshal(map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	valBytes, err := json.Marshal(map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	message := &Message{
		Headers: []MessageHeader{{
			"key",
			keyHeaderBytes,
		}, {
			"val",
			valHeaderBytes,
		}},
		Key:   keyBytes,
		Value: valBytes,
	}
	return message, nil
}

func NewStringKeyProtobufValueEncoderFactory(registry protolib.Resolver) func(options string) (MessageEncoder, error) {
	return func(options string) (MessageEncoder, error) {
		return NewStringKeyProtobufValueEncoder(registry, options)
	}
}

func NewStringKeyProtobufValueEncoder(protoRegistry protolib.Resolver, options string) (MessageEncoder, error) {
	name := pref.FullName(options)
	desc, err := protoRegistry.FindDescriptorByName(name)
	if err != nil {
		return nil, err
	}
	mdesc, ok := desc.(pref.MessageDescriptor)
	if !ok {
		return nil, errors.Errorf("expected %q to be a MessageDescriptor, but was %v", name, reflect.TypeOf(mdesc))
	}
	return &StringKeyProtobufValueEncoder{d: mdesc}, nil
}

// StringKeyProtobufValueEncoder is an encoder that translates each row to a protobuf message. Columns 0 to N correspond
// to protobuf field numbers 1 to N+1. This means that the key also ends up as a field in the value protobuf.
type StringKeyProtobufValueEncoder struct {
	d pref.MessageDescriptor
}

func (e *StringKeyProtobufValueEncoder) Name() string {
	return "StringKeyProtobufValueEncoder"
}

func (e *StringKeyProtobufValueEncoder) EncodeMessage(row *common.Row, colTypes []common.ColumnType, keyCols []int, timestamp time.Time) (*Message, error) {
	if len(keyCols) != 1 {
		return nil, errors.New("must be only one pk col for binary key encoding")
	}
	keyColIndex := keyCols[0]
	keyColType := colTypes[keyColIndex]
	if keyColType != common.VarcharColumnType {
		return nil, errors.New("Key is not a varchar column")
	}
	keyBytes := []byte(row.GetString(keyColIndex))

	valBytes, err := e.encodeProtobufValue(colTypes, row)
	if err != nil {
		return nil, err
	}

	return &Message{
		Key:   keyBytes,
		Value: valBytes,
	}, nil
}

func (e *StringKeyProtobufValueEncoder) encodeProtobufValue(colTypes []common.ColumnType, row *common.Row) ([]byte, error) {
	msg := dynamicpb.NewMessage(e.d)
	fields := e.d.Fields()
	for i, colType := range colTypes {
		colVal, err := getColVal(i, colType, row)
		if err != nil {
			return nil, err
		}
		protoSet(msg, fields.ByNumber(pref.FieldNumber(i+1)), colVal)
	}
	return proto.Marshal(msg)
}

func protoSet(msg *dynamicpb.Message, fd pref.FieldDescriptor, v interface{}) {
	// coerce types when necessary and possible. If not possible, let the protoreflect library panic
	// with a nice message
	switch t := v.(type) {
	case float64:
		if fd.Kind() == pref.FloatKind {
			v = float32(t)
		}
	case int64:
		switch fd.Kind() {
		case pref.Int32Kind, pref.Sint32Kind, pref.Sfixed32Kind:
			v = int32(t)
		case pref.Uint32Kind, pref.Fixed32Kind:
			v = uint32(t)
		case pref.Uint64Kind, pref.Fixed64Kind:
			v = uint64(t)
		case pref.BoolKind:
			v = t != 0
		case pref.EnumKind:
			v = pref.EnumNumber(t)
		default:
			// fallthrough
		}
	case string:
		if fd.Kind() == pref.BytesKind {
			v = []byte(t)
		}
	default:
		panic(fmt.Sprintf("unknown type %s", reflect.TypeOf(v)))
	}
	msg.Set(fd, pref.ValueOf(v))
}
