package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/common"
	"time"
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
