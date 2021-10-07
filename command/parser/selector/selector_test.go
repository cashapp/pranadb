package selector

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/testproto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

var fd = testproto.File_squareup_cash_pranadb_testproto_v1_testproto_proto

func TestParseSelector(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		want     Selector
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

func newSelector(s ...interface{}) Selector {
	sel := make(Selector, 0, len(s))
	for _, p := range s {
		switch v := p.(type) {
		case string:
			sel = append(sel, Path{Field: &v})
		case int:
			sel = append(sel, Path{NumberIndex: &v})
		default:
			panic("invalid selector")
		}
	}
	return sel
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
		{name: "partial oneof selector", selector: "oneof_field", want: "one_int64"},
		{name: "one of", selector: "oneof_field.one_int64", want: int64(1000)},
		// respect that protos are nice about nil values
		{name: "nil dereference", selector: "recursive_field.recursive_field.recursive_field.recursive_field.string_field", want: ""},
		{name: "invalid field", selector: "recursive_field.not_a_field.nope", wantErr: &ErrNotFound{}, wantErrMsg: "Value at \"recursive_field.not_a_field\" not found while looking for \"recursive_field.not_a_field.nope\""},
		{name: "index list using string", selector: "repeated_string_field[\"hello\"]", wantErr: errors.New(""), wantErrMsg: "cannot index list at \"repeated_string_field.hello\" with string"},
		{name: "index list twice", selector: "repeated_string_field[1][2]", wantErr: errors.New(""), wantErrMsg: "cannot get index 2 of \"string\""},
		{name: "index string map using int", selector: "string_map_field[88]", wantErr: errors.New(""), wantErrMsg: "cannot convert int to map key of kind \"string\" at \"string_map_field\""},
		{name: "index string map twice", selector: "string_map_field[\"lois\"][\"nope\"]", wantErr: errors.New(""), wantErrMsg: "cannot get field \"nope\" of \"string\""},
		{name: "index into oneof field", selector: "oneof_field[3]", wantErr: errors.New(""), wantErrMsg: "cannot get index 3 of oneof field at \"oneof_field\""},
		{name: "enum", selector: "enum_field", want: dynamicpb.NewEnumType(fd.Enums().ByName("Count")).New(1)},
		{name: "invalid oneof", selector: "oneof_field.one_wibble", wantErr: errors.New(""), wantErrMsg: "unknown oneof field \"one_wibble\""},
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
