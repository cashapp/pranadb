package selector

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/testproto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

var fd = testproto.File_squareup_cash_pranadb_testproto_v1_testproto_proto

func TestParseColumnSelector(t *testing.T) {
	stringRef := func(v string) *string {
		return &v
	}
	tests := []struct {
		name     string
		selector string
		want     ColumnSelector
		wantErr  bool
	}{
		{
			name:     "dot select",
			selector: "hello.great.world",
			want:     ColumnSelector{Selector: newSelector("hello", "great", "world")},
		},
		{
			name:     "array index",
			selector: `hello[3]`,
			want:     ColumnSelector{Selector: newSelector("hello", 3)},
		},
		{
			name:     "deep indexing",
			selector: `hello.great["5"].world`,
			want:     ColumnSelector{Selector: newSelector("hello", "great", "5", "world")},
		},
		{
			name:     "complicated",
			selector: `hello[0][1]["2"]["world"].foo[3]`,
			want:     ColumnSelector{Selector: newSelector("hello", 0, 1, "2", "world", "foo", 3)},
		},
		{
			name:     "meta",
			selector: `meta("key").hello.world`,
			want:     ColumnSelector{MetaKey: stringRef("key"), Selector: newSelector("hello", "world")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sel, err := ParseColumnSelector(test.selector)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, sel)
			}
		})
	}
}

func TestParseSelector(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		want     SelectorInjector
		wantErr  bool
	}{
		{
			name:     "dot select",
			selector: "hello.great.world",
			want:     newSelector("hello", "great", "world"),
		},
		{
			name:     "array index",
			selector: `hello[3]`,
			want:     newSelector("hello", 3),
		},
		{
			name:     "deep indexing",
			selector: `hello.great["5"].world`,
			want:     newSelector("hello", "great", "5", "world"),
		},
		{
			name:     "complicated",
			selector: `hello[0][1]["2"]["world"].foo[3]`,
			want:     newSelector("hello", 0, 1, "2", "world", "foo", 3),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sel, err := ParseSelector(test.selector)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, sel)
			}
		})
	}
}

func newSelector(s ...interface{}) SelectorInjector {
	sel := make(SelectorInjector, 0, len(s))
	for _, p := range s {
		switch v := p.(type) {
		case string:
			sel = append(sel, Path{Field: &v})
		case int:
			sel = append(sel, Path{NumberIndex: &v})
		default:
			panic("invalid selectorInjector")
		}
	}
	return sel
}

func TestInject(t *testing.T) {

	tests := []struct {
		name             string
		selectorInjector string
		val              interface{}
	}{
		{name: "number", selectorInjector: "number", val: 123.456},
		{name: "string", selectorInjector: "string", val: "hello world"},
		{name: "list", selectorInjector: "list", val: []interface{}{float64(345), "foo"}},
		{name: "number in list", selectorInjector: "list[0]", val: float64(345)},
		{name: "string in list", selectorInjector: "list[1]", val: "foo"},
		{name: "nested string", selectorInjector: "nested.string", val: "a nested string"},
		{name: "object in list", selectorInjector: "objlist[1].tues", val: float64(2)},
		{name: "list as object member", selectorInjector: "foo.objlist[1].tues", val: float64(2)},
		{name: "object in list map index syntax", selectorInjector: `objlist[1]["tues"]`, val: float64(2)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sel, err := ParseSelector(test.selectorInjector)
			require.NoError(t, err)
			data := map[string]interface{}{}
			err = sel.Inject(data, test.val)
			require.NoError(t, err)
			actual, err2 := sel.Select(data)
			require.NoError(t, err2)
			require.Equal(t, test.val, actual)
		})
	}
}

func TestInjectArrayIndexesDescendingOrder(t *testing.T) {
	data := map[string]interface{}{}
	for i := 4; i >= 0; i-- {
		sel, err := ParseSelector(fmt.Sprintf("foo.some_array[%d]", i))
		require.NoError(t, err)
		err = sel.Inject(data, float64(i))
		require.NoError(t, err)
	}
	sel2, err := ParseSelector("foo.some_array")
	require.NoError(t, err)
	arr, err := sel2.Select(data)
	require.NoError(t, err)
	require.Equal(t, []interface{}{float64(0), float64(1), float64(2), float64(3), float64(4)}, arr)
}

func TestInjectArrayIndexesAscendingOrderFails(t *testing.T) {
	data := map[string]interface{}{}
	sel, err := ParseSelector("foo.some_array[2]")
	require.NoError(t, err)
	err = sel.Inject(data, 23)
	require.NoError(t, err)
	sel, err = ParseSelector("foo.some_array[3]")
	require.NoError(t, err)
	err = sel.Inject(data, 23)
	require.Error(t, err)
}

func TestSelect(t *testing.T) {
	rawJSON := `{
  "number": 123.456,
  "string": "hello world",
  "list": [345, "foo"],
  "nested": {
    "string": "a nested string"
  },
  "objlist": [{"mon": 1}, {"tues": 2}]
}`
	var data map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(rawJSON), &data))
	data["proto"] = &testproto.TestTypes{
		StringField: "proto string",
	}

	tests := []struct {
		name       string
		selector   string
		want       interface{}
		wantErr    error
		wantErrMsg string
	}{
		{name: "number", selector: "number", want: 123.456},
		{name: "string", selector: "string", want: "hello world"},
		{name: "list", selector: "list", want: []interface{}{float64(345), "foo"}},
		{name: "number in list", selector: "list[0]", want: float64(345)},
		{name: "string in list", selector: "list[1]", want: "foo"},
		{name: "nested string", selector: "nested.string", want: "a nested string"},
		{name: "object in list", selector: "objlist[1].tues", want: float64(2)},
		{name: "object in list map index syntax", selector: `objlist[1]["tues"]`, want: float64(2)},
		{name: "list out of bounds", selector: "list[2]", wantErr: &ErrNotFound{}, wantErrMsg: "Value at \"list[2]\" not found while looking for \"list[2]\""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sel, err := ParseSelector(test.selector)
			require.NoError(t, err)
			v, err := sel.Select(data)
			if test.wantErr != nil {
				require.Error(t, err)
				require.ErrorAs(t, err, &test.wantErr)
				require.Equal(t, test.wantErrMsg, test.wantErr.Error())
			} else {
				require.Equal(t, test.want, v)
			}
		})
	}
}

func TestSelectProto(t *testing.T) {
	data := &testproto.TestTypes{
		DoubleField: 1.2,
		FloatField:  2.3,
		Int32Field:  1234,
		Int64Field:  12345678,
		Uint32Field: 2345,
		Uint64Field: 23456789,
		BoolField:   true,
		StringField: "hello world",
		BytesField:  []byte("good morning"),
		NestedField: &testproto.TestTypes_Nested{
			NestedString:         "good evening",
			NestedRepeatedString: []string{"red", "blue", "orange"},
			NestedMap: map[string]string{
				"monday":  "1",
				"tuesday": "2",
			},
		},
		RepeatedStringField: []string{"one", "two", "three"},
		RecursiveField: &testproto.Recursive{
			StringField: "nine",
			RecursiveField: &testproto.Recursive{
				StringField: "ten",
			},
		},
		OneofField: &testproto.TestTypes_OneInt64{OneInt64: 1000},
		StringMapField: map[string]string{
			"barry": "seymour",
			"lois":  "lane",
		},
		IntMapField: map[int32]string{
			88: "eighty-eight",
			99: "ninety-nine",
		},
		MapMessageField: map[string]*testproto.SimpleValue{
			"batman":   {Value: "robin"},
			"superman": {Value: "flash"},
		},
		EnumField: testproto.Count_COUNT_ONE,
	}
	bin, err := proto.Marshal(data)
	require.NoError(t, err)
	dynmsg := dynamicpb.NewMessage(data.ProtoReflect().Descriptor())
	err = proto.Unmarshal(bin, dynmsg)
	require.NoError(t, err)

	tests := []struct {
		name       string
		selector   string
		want       interface{}
		wantErr    error
		wantErrMsg string
	}{
		{name: "basic", selector: "double_field", want: 1.2},
		{name: "list", selector: "repeated_string_field[1]", want: "two"},
		{name: "deep", selector: "nested_field.nested_string", want: "good evening"},
		{name: "recursive", selector: "recursive_field.recursive_field.string_field", want: "ten"},
		{name: "nested map", selector: "nested_field.nested_map[\"monday\"]", want: "1"},
		{name: "nested repeated string", selector: "nested_field.nested_repeated_string[2]", want: "orange"},
		{name: "string map", selector: "string_map_field[\"lois\"]", want: "lane"},
		{name: "int map", selector: "int_map_field[88]", want: "eighty-eight"},
		{name: "map message", selector: "map_message_field[\"batman\"].value", want: "robin"},
		{name: "map message indexing", selector: "map_message_field[\"batman\"][\"value\"]", want: "robin"},
		{name: "partial oneof selectorInjector", selector: "oneof_field", want: "one_int64"},
		{name: "one of", selector: "oneof_field.one_int64", want: int64(1000)},
		// respect that protos are nice about nil values
		{name: "nil dereference", selector: "recursive_field.recursive_field.recursive_field.recursive_field.string_field", want: ""},
		{name: "invalid field", selector: "recursive_field.not_a_field.nope", wantErr: &ErrNotFound{}, wantErrMsg: "Value at \"recursive_field.not_a_field\" not found while looking for \"recursive_field.not_a_field.nope\""},
		{name: "index list using string", selector: "repeated_string_field[\"hello\"]", wantErr: errors.Error(""), wantErrMsg: "cannot index list at \"repeated_string_field.hello\" with string"},
		{name: "index list twice", selector: "repeated_string_field[1][2]", wantErr: errors.Error(""), wantErrMsg: "cannot get index 2 of \"string\""},
		{name: "index string map using int", selector: "string_map_field[88]", wantErr: errors.Error(""), wantErrMsg: "cannot convert int to map key of kind \"string\" at \"string_map_field\""},
		{name: "index string map twice", selector: "string_map_field[\"lois\"][\"nope\"]", wantErr: errors.Error(""), wantErrMsg: "cannot get field \"nope\" of \"string\""},
		{name: "index into oneof field", selector: "oneof_field[3]", wantErr: errors.Error(""), wantErrMsg: "cannot get index 3 of oneof field at \"oneof_field\""},
		{name: "enum", selector: "enum_field", want: dynamicpb.NewEnumType(fd.Enums().ByName("Count")).New(1)},
		{name: "invalid oneof", selector: "oneof_field.one_wibble", wantErr: errors.Error(""), wantErrMsg: "unknown oneof field \"one_wibble\""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sel, err := ParseSelector(test.selector)
			require.NoError(t, err)
			v, err := sel.Select(dynmsg)
			if test.wantErr != nil {
				require.Error(t, err)
				require.ErrorAs(t, err, &test.wantErr)
				require.Equal(t, test.wantErrMsg, test.wantErr.Error())
			} else {
				require.Equal(t, test.want, v)
			}
		})
	}
}

func TestSelectorInjector_InjectProto(t *testing.T) {
	var p testproto.ComprehensiveTestMessage
	option2 := dynamicpb.NewEnumType(p.ProtoReflect().Descriptor().Enums().ByName("TestEnum")).New(2)
	tests := []struct {
		name        string
		selector    string
		injectValue interface{}
		value       interface{}
		wantErrMsg  string
	}{
		// scalar
		{name: "top level scalar double", selector: "double_field", value: float64(1.7)},
		{name: "top level scalar float", selector: "float_field", value: float32(-2.9)},
		{name: "top level scalar int32", selector: "int32_field", value: int32(-18)},
		{name: "top level scalar int64", selector: "int64_field", value: int64(-37)},
		{name: "top level scalar uint32", selector: "uint32_field", value: uint32(18)},
		{name: "top level scalar uint64", selector: "uint64_field", value: uint64(37)},
		{name: "top level scalar sint32", selector: "sint32_field", value: int32(-11)},
		{name: "top level scalar sint64", selector: "sint64_field", value: int64(24)},
		{name: "top level scalar fixed32", selector: "fixed32_field", value: uint32(19)},
		{name: "top level scalar fixed64", selector: "fixed64_field", value: uint64(7)},
		{name: "top level scalar sfixed32", selector: "sfixed32_field", value: int32(-1)},
		{name: "top level scalar sfixed64", selector: "sfixed64_field", value: int64(3)},
		{name: "top level scalar bool true", selector: "bool_field", value: true},
		{name: "top level scalar bool false", selector: "bool_field", value: false},
		{name: "top level scalar string empty", selector: "string_field", value: ""},
		{name: "top level scalar string non empty", selector: "string_field", value: "foo"},
		{name: "top level scalar bytes empty", selector: "bytes_field", value: []byte(nil)},
		{name: "top level scalar bytes non empty", selector: "bytes_field", value: []byte("foo")},

		// enum
		{name: "top level enum integer", selector: "enum_field", value: option2, injectValue: 2},
		{name: "top level enum string", selector: "enum_field", value: option2, injectValue: "TEST_ENUM_OPTION_2"},
		{name: "nested nested enum integer", selector: "recursive_field.recursive_field.enum_field", value: option2, injectValue: 2},
		{name: "nested nested enum string", selector: "recursive_field.recursive_field.enum_field", value: option2, injectValue: "TEST_ENUM_OPTION_2"},

		// message
		{name: "nested scalar", selector: "recursive_field.string_field", value: "foobar"},
		{name: "nested nested scalar", selector: "recursive_field.recursive_field.string_field", value: "foobar"},

		// oneof
		{name: "top level one of int64", selector: "oneof_field.oneof_int64_field", value: int64(10)},
		{name: "top level one of bool true", selector: "oneof_field.oneof_bool_field", value: true},
		{name: "top level one of bool false", selector: "oneof_field.oneof_bool_field", value: false},
		{name: "top level one of string", selector: "oneof_field.oneof_string_field", value: "foo"},
		{name: "top level one of nested nested string", selector: "oneof_field.oneof_recursive_field.recursive_field.string_field", value: "foo"},
		{name: "nested one of nested string", selector: "recursive_field.oneof_field.oneof_recursive_field.oneof_field.oneof_string_field", value: "foo"},

		// map
		{name: "top level map string int", selector: "map_string_int_field[\"foo\"]", value: int64(19)},
		{name: "top level map int string", selector: "map_int_string_field[7]", value: "bar"},
		{name: "top level map string recursive", selector: "map_string_recursive_field[\"foo\"].string_field", value: "bar"},
		{name: "top level map string enum int", selector: "map_string_enum_field[\"foo\"]", value: option2, injectValue: 2},
		{name: "top level map string enum string", selector: "map_string_enum_field[\"foo\"]", value: option2, injectValue: "TEST_ENUM_OPTION_2"},
		{name: "map string recursive x2", selector: "map_string_recursive_field[\"foo\"].map_string_recursive_field[\"bar\"].string_field", value: "foobar"},
		{name: "nested map string recursive", selector: "recursive_field.map_string_recursive_field[\"bar\"].string_field", value: "foobar"},

		// repeated
		{name: "top level repeated int", selector: "repeated_int64_field[3]", value: int64(51)},
		{name: "top level repeated recursive", selector: "repeated_recursive_field[3].string_field", value: "foo"},
		{name: "repeated recursive x2", selector: "repeated_recursive_field[3].repeated_recursive_field[1].string_field", value: "bar"},
		{name: "nested repeated recursive", selector: "recursive_field.repeated_recursive_field[1].string_field", value: "bar"},

		// mix
		{name: "mix", selector: "recursive_field.oneof_field.oneof_recursive_field.repeated_recursive_field[1].map_string_recursive_field[\"foo\"].string_field", value: "baz"},

		// errors
		{name: "error top level unknown field", selector: "foo", value: "bar", wantErrMsg: "Value at \"foo\" not found while looking for \"foo\""},
		{name: "error nested unknown field", selector: "recursive_field.recursive_field.foo", value: "bar", wantErrMsg: "Value at \"foo\" not found while looking for \"recursive_field.recursive_field.foo\""},
		{name: "error nested list unknown field", selector: "recursive_field.repeated_recursive_field[0].foo", value: "bar", wantErrMsg: "Value at \"foo\" not found while looking for \"recursive_field.repeated_recursive_field[0].foo\""},
		{name: "error nested map unknown field", selector: "recursive_field.map_int_string_field[0].foo", value: "bar", wantErrMsg: "Value at \"foo\" not found while looking for \"recursive_field.map_int_string_field[0].foo\""},
		{name: "error extra field scalar", selector: "recursive_field.repeated_recursive_field[0].string_field.foo", value: "bar", wantErrMsg: "Value at \"foo\" not found while looking for \"recursive_field.repeated_recursive_field[0].string_field.foo\""},
		{name: "error extra fields scalar", selector: "recursive_field.repeated_recursive_field[0].string_field.foo.bar", value: "bar", wantErrMsg: "Value at \"foo.bar\" not found while looking for \"recursive_field.repeated_recursive_field[0].string_field.foo.bar\""},
		{name: "error extra fields scalar list", selector: "recursive_field.repeated_int64_field[0].foo", value: "bar", wantErrMsg: "Value at \"foo\" not found while looking for \"recursive_field.repeated_int64_field[0].foo\""},
		{name: "error extra fields scalar map", selector: "recursive_field.map_int_string_field[7].foo", value: "bar", wantErrMsg: "Value at \"foo\" not found while looking for \"recursive_field.map_int_string_field[7].foo\""},
		{name: "error index list with string", selector: "repeated_int64_field[\"foo\"]", value: 10, wantErrMsg: "cannot index list with string \"foo\""},
		{name: "error nested index list with string", selector: "recursive_field.repeated_int64_field[\"foo\"]", value: 10, wantErrMsg: "cannot index list with string \"foo\""},
		{name: "error set list", selector: "recursive_field.repeated_int64_field", value: 10, wantErrMsg: "selector \"recursive_field.repeated_int64_field\" does not point to a scalar value"},
		{name: "error set map", selector: "recursive_field.map_string_int_field", value: 10, wantErrMsg: "selector \"recursive_field.map_string_int_field\" does not point to a scalar value"},
		{name: "error set message", selector: "recursive_field.recursive_field", value: 10, wantErrMsg: "selector \"recursive_field.recursive_field\" does not point to a scalar value"},
		{name: "error enum non string or int", selector: "enum_field", value: float32(1.1), wantErrMsg: "enum value must be either int or string"},
		{name: "error int selector for message", selector: "recursive_field[10].x", value: 2, wantErrMsg: "selector \"[10].x\" should be a string in \"recursive_field[10].x\""},
		{name: "error int selector for one of", selector: "oneof_field[10].x", value: 2, wantErrMsg: "selector \"[10].x\" should be a string in \"oneof_field[10].x\""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var proto testproto.ComprehensiveTestMessage
			inj, err := ParseSelector(test.selector)
			require.NoError(t, err)
			msg := proto.ProtoReflect()
			val := test.value
			if test.injectValue != nil {
				val = test.injectValue
			}
			err = inj.Inject(msg, val)
			if test.wantErrMsg != "" {
				require.Error(t, err)
				require.Equal(t, test.wantErrMsg, err.Error())
			} else {
				require.NoError(t, err)
				gotVal, err := inj.Select(msg)
				require.NoError(t, err)
				require.Equal(t, test.value, gotVal)
			}
		})
	}
}
