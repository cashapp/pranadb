package source

import (
	"github.com/squareup/pranadb/command/parser/selector"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/push/codec"
	"github.com/squareup/pranadb/push/util"
)

const (
	tinyIntMinVal    = -128
	tinyIntMaxVal    = 127
	intMinVal        = -2147483648
	intMaxVal        = 2147483647
	maxVarcharLength = 65535
)

type MessageParser struct {
	sourceInfo       *common.SourceInfo
	rowsFactory      *common.RowsFactory
	selectors        []selector.ColumnSelector
	headerDecoder    codec.Codec
	keyDecoder       codec.Codec
	valueDecoder     codec.Codec
	evalContext      *evalContext
	protobufRegistry protolib.Resolver
}

func NewMessageParser(sourceInfo *common.SourceInfo, registry protolib.Resolver) (*MessageParser, error) {
	selectors := sourceInfo.OriginInfo.ColSelectors
	topic := sourceInfo.OriginInfo
	headerCodec, keyCodec, valueCodec, err := util.GetCodecs(registry, topic.HeaderEncoding, topic.KeyEncoding, topic.ValueEncoding, selectors)
	if err != nil {
		return nil, err
	}
	mp := &MessageParser{
		rowsFactory:      common.NewRowsFactory(sourceInfo.ColumnTypes),
		protobufRegistry: registry,
		sourceInfo:       sourceInfo,
		selectors:        selectors,
		headerDecoder:    headerCodec,
		keyDecoder:       keyCodec,
		valueDecoder:     valueCodec,
		evalContext: &evalContext{
			meta: make(map[string]interface{}, 3),
		},
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

func (m *MessageParser) evalColumns(rows *common.Rows) error { //nolint:gocyclo
	for i, sel := range m.selectors {
		colType := m.sourceInfo.ColumnTypes[i]
		eval := sel.Select
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
			ival, err := codec.CoerceInt64(val)
			if err != nil {
				return errors.WithStack(err)
			}
			if ival < tinyIntMinVal || ival > tinyIntMaxVal {
				return errors.NewPranaErrorf(errors.ValueOutOfRange,
					"value %d is out of range for TINYINT (%d < x < %d) in source %s.%s",
					ival, tinyIntMinVal, tinyIntMaxVal, m.sourceInfo.SchemaName, m.sourceInfo.Name)
			}
			rows.AppendInt64ToColumn(i, ival)
		case common.TypeInt:
			ival, err := codec.CoerceInt64(val)
			if err != nil {
				return errors.WithStack(err)
			}
			if ival < intMinVal || ival > intMaxVal {
				return errors.NewPranaErrorf(errors.ValueOutOfRange,
					"value %d is out of range for INT (%d < x < %d) in source %s.%s",
					ival, intMinVal, intMaxVal, m.sourceInfo.SchemaName, m.sourceInfo.Name)
			}
			rows.AppendInt64ToColumn(i, ival)
		case common.TypeBigInt:
			ival, err := codec.CoerceInt64(val)
			if err != nil {
				return errors.WithStack(err)
			}
			rows.AppendInt64ToColumn(i, ival)
		case common.TypeDouble:
			fval, err := codec.CoerceFloat64(val)
			if err != nil {
				return errors.WithStack(err)
			}
			rows.AppendFloat64ToColumn(i, fval)
		case common.TypeVarchar:
			sval, err := codec.CoerceString(val)
			if err != nil {
				return errors.WithStack(err)
			}
			if len(sval) > getMaxVarcharLength() {
				return errors.NewPranaErrorf(errors.VarcharTooBig,
					"value is too long at %d for varchar (max length %d) in source %s.%s",
					len(sval), maxVarcharLength, m.sourceInfo.SchemaName, m.sourceInfo.Name)
			}
			rows.AppendStringToColumn(i, sval)
		case common.TypeDecimal:
			dval, err := codec.CoerceDecimal(val)
			if err != nil {
				return errors.WithStack(err)
			}
			rows.AppendDecimalToColumn(i, *dval)
		case common.TypeTimestamp:
			tsVal, err := codec.CoerceTimestamp(val)
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

func (m *MessageParser) decodeBytes(decoder codec.Codec, bytes []byte) (interface{}, error) {
	if bytes == nil {
		return nil, nil
	}
	v, err := decoder.Decode(bytes)
	return v, errors.WithStack(err)
}

type evalContext struct {
	meta  map[string]interface{}
	value interface{}
}

func getMaxVarcharLength() int {
	if MaxVarCharOverride != -1 {
		return MaxVarCharOverride
	}
	return maxVarcharLength
}

var MaxVarCharOverride = -1
