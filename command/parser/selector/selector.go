// Package selector contains a selector library for JSON and Protobuf.
// It's in its own standalone package only to avoid circular dependencies
//
// nolint:govet
package selector

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/alecthomas/participle/v2/lexer/stateful"
	"github.com/squareup/pranadb/perrors"

	"github.com/alecthomas/participle/v2"
	pref "google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type ColumnSelectorAST struct {
	MetaKey *string      `( "meta" "(" @String ")" |`
	Field   *string      `  @Ident )`
	Index   []*Index     `( "[" @@ "]" )*`
	Next    *SelectorAST `("." @@)?`
}

func (s *ColumnSelectorAST) ToSelector() ColumnSelector {
	if s.MetaKey != nil {
		return ColumnSelector{
			MetaKey:  s.MetaKey,
			Selector: s.Next.ToSelector(),
		}
	}
	f := ""
	if s.Field != nil {
		f = *s.Field
	}
	return ColumnSelector{
		MetaKey: s.MetaKey,
		Selector: (&SelectorAST{
			Field: f,
			Index: s.Index,
			Next:  s.Next,
		}).ToSelector(),
	}
}

type ColumnSelector struct {
	MetaKey  *string
	Selector Selector
}

func (s *ColumnSelector) Select(meta map[string]interface{}, body interface{}) (interface{}, error) {
	if s.MetaKey != nil {
		v, ok := meta[*s.MetaKey]
		if !ok {
			return nil, fmt.Errorf("metadata did not contain key %q", *s.MetaKey)
		}
		return s.Selector.Select(v)
	}
	return s.Selector.Select(body)
}

func (s ColumnSelector) String() string {
	var v string
	if s.MetaKey != nil {
		v += fmt.Sprintf(`meta("%s")`, *s.MetaKey)
	}
	if len(s.Selector) > 0 {
		v += "." + s.Selector.String()
	}
	return v
}

type SelectorAST struct {
	Field string       `@Ident`
	Index []*Index     `( "[" @@ "]" )*`
	Next  *SelectorAST `("." @@)?`
}

type Index struct {
	Number *int    `@Number |`
	String *string `@String`
}

func (a *SelectorAST) ToSelector() Selector {
	var sel []Path
	for ; a != nil; a = a.Next {
		sel = append(sel, Path{Field: &a.Field})
		if len(a.Index) > 0 {
			for _, idx := range a.Index {
				if idx.Number != nil {
					sel = append(sel, Path{NumberIndex: idx.Number})
				} else {
					sel = append(sel, Path{Field: idx.String})
				}
			}
		}
	}
	return sel
}

// Selector is a protobuf path selector.
type Selector []Path

type Path struct {
	Field       *string
	NumberIndex *int
}

// ErrNotFound is returned when the selector references an invalid field
type ErrNotFound struct {
	missingPath Selector
	targetPath  Selector
}

// Error returns the formatted error string
func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("Value at %q not found while looking for %q", e.missingPath, e.targetPath)
}

var lex = stateful.MustSimple([]stateful.Rule{
	{`Ident`, "((?i)[a-zA-Z_][a-zA-Z_0-9]*)|`[^`]*`", nil},
	{`Number`, `[-+]?\d*\.?\d+([eE][-+]?\d+)?`, nil},
	{`String`, `'[^']*'|"[^"]*"`, nil},
	{`Punct`, `<>|!=|<=|>=|\]|\[|[-+*/%,.()=<>;]`, nil},
	{`Whitespace`, `\s+`, nil},
})
var selectorParser = participle.MustBuild(&SelectorAST{},
	participle.Lexer(lex),
	participle.Unquote("String"),
)
var columnSelectorParser = participle.MustBuild(&ColumnSelectorAST{},
	participle.Lexer(lex),
	participle.Unquote("String"),
)

// ParseColumnSelector parses a selector expression into an executable ColumnSelector.
func ParseColumnSelector(str string) (ColumnSelector, error) {
	s := &ColumnSelectorAST{}
	err := columnSelectorParser.ParseString("", str, s)
	return s.ToSelector(), err
}

// ParseSelector parses a selector expression into an executable Selector.
func ParseSelector(str string) (Selector, error) {
	s := &SelectorAST{}
	err := selectorParser.ParseString("", str, s)
	return s.ToSelector(), err
}

// Select evaluates the selector expression against the given value. The only supported types are
//     map[string]interface{}
//     []interface{}
//     google.golang.org/protobuf/reflect.Message
func (s Selector) Select(v interface{}) (interface{}, error) {
	var ok bool
	for i, token := range s {
		switch vv := v.(type) {
		case map[string]interface{}:
			if token.NumberIndex != nil {
				return nil, perrors.Errorf("cannot use string to index map with number key at %q", s[0:i+1])
			}
			v, ok = vv[*token.Field]
			if !ok {
				return nil, nil
			}
		case []interface{}:
			if token.Field != nil {
				return nil, perrors.Errorf("cannot index array using %q at %q", *token.Field, s[0:i+1])
			}
			if len(vv) <= *token.NumberIndex {
				return nil, &ErrNotFound{s[0 : i+1], s}
			}
			v = vv[*token.NumberIndex]
		case pref.Message:
			return s[i:].SelectProto(vv)
		}
	}
	return v, nil
}

// SelectProto returns the referenced value from the protobuf message. Adhering to Golang protobuf behavior, if a selector
// references nested value of a nil message, the default Go value will be returned. Array out of index will still panic.
// ErrNotFound is returned if a non-existing field is referenced. Other errors may be returned on failed type conversion.
// nolint: gocyclo
func (s Selector) SelectProto(msg pref.Message) (interface{}, error) {
	if len(s) == 0 {
		return msg, nil
	}
	v, f, oneOf, ok := getField(msg, *s[0].Field)
	if !ok {
		return nil, &ErrNotFound{missingPath: s[0:1], targetPath: s}
	}
	var err error
	for i, token := range s[1:] {
		tail := i + 2
		switch { // order matters!
		case oneOf != nil:
			msg = v.Message()
			if token.NumberIndex != nil {
				return nil, perrors.Errorf("cannot get index %d of oneof field at %q", *token.NumberIndex, s[0:tail-1])
			}
			f = oneOf.Fields().ByName(pref.Name(*token.Field))
			if f == nil {
				return nil, perrors.Errorf("unknown oneof field \"%s\"", *token.Field)
			}
			populated := msg.WhichOneof(oneOf)
			if populated.Number() != f.Number() {
				// Different one_of field than the one being accessed is populated.
				return nil, &ErrNotFound{missingPath: s[0:tail], targetPath: s}
			}
			v = msg.Get(f)
			oneOf = nil
		case token.NumberIndex != nil:
			idx := *token.NumberIndex
			switch {
			case f.IsList():
				v = v.List().Get(idx)
				f = noRepeatField{f}
			case f.IsMap():
				var k pref.MapKey
				k = newIntMapKey(f.MapKey(), idx)
				if !k.IsValid() {
					return nil, perrors.Errorf("cannot convert int to map key of kind %q at %q", f.MapKey().Kind(), s[0:tail-1])
				}
				if err != nil {
					return nil, err
				}
				v = v.Map().Get(k)
				f = f.MapValue()
			default:
				return nil, perrors.Errorf("cannot get index %d of %q", idx, f.Kind())
			}
		case token.Field != nil:
			fieldName := *token.Field
			switch {
			case f.IsMap():
				if f.MapKey().Kind() != pref.StringKind {
					return nil, perrors.Errorf("cannot use string to index map with %q key", f.MapKey().Kind())
				}
				v = v.Map().Get(pref.ValueOfString(fieldName).MapKey())
				f = f.MapValue()
			case f.IsList():
				return nil, perrors.Errorf("cannot index list at %q with string", s[0:tail])
			case f.Message() != nil:
				var ok bool
				v, f, oneOf, ok = getField(v.Message(), fieldName)
				if !ok {
					return nil, &ErrNotFound{missingPath: s[0:tail], targetPath: s}
				}
			default:
				return nil, perrors.Errorf("cannot get field %q of %q", fieldName, f.Kind())
			}
		default:
			panic("invalid path token")
		}
	}
	if oneOf != nil {
		// When the selector terminates on a oneof field we return the name of the field as the value
		// or nil if the oneof field is not there
		f := v.Message().WhichOneof(oneOf)
		if f == nil {
			return nil, nil
		}
		return string(f.Name()), nil
	}
	ret := v.Interface()
	if r := reflect.ValueOf(ret); r.Type().Kind() == reflect.Ptr && r.IsNil() {
		return nil, nil
	}
	if e, ok := ret.(pref.EnumNumber); ok && f.Enum() != nil {
		return dynamicpb.NewEnumType(f.Enum()).New(e), nil
	}
	return ret, nil
}

func newIntMapKey(keyDesc pref.FieldDescriptor, k int) pref.MapKey {
	var v pref.Value
	switch keyDesc.Kind() {
	case pref.Int32Kind, pref.Sint32Kind, pref.Sfixed32Kind:
		v = pref.ValueOfInt32(int32(k))
	case pref.Int64Kind, pref.Sint64Kind, pref.Sfixed64Kind:
		v = pref.ValueOfInt64(int64(k))
	case pref.Uint32Kind, pref.Fixed32Kind:
		v = pref.ValueOfUint32(uint32(k))
	case pref.Uint64Kind, pref.Fixed64Kind:
		v = pref.ValueOfUint64(uint64(k))
	default:
		return pref.MapKey{}
	}
	return v.MapKey()
}

func (s Selector) String() string {
	sb := &strings.Builder{}
	for i, p := range s {
		if p.Field != nil {
			if i != 0 {
				sb.WriteRune('.')
			}
			sb.WriteString(*p.Field)
		} else if p.NumberIndex != nil {
			sb.WriteRune('[')
			sb.WriteString(strconv.Itoa(*p.NumberIndex))
			sb.WriteRune(']')
		}
	}
	return sb.String()
}

// getField returns the targeted message field's Value and FieldDescriptor. If the field is an one_of field, returns
// the current message as a Value and the OneOfDescriptor so that the actual field can be selected when processing
// the next segment of the path.
func getField(msg pref.Message, name string) (pref.Value, pref.FieldDescriptor, pref.OneofDescriptor, bool) {
	field := msg.Descriptor().Fields().ByName(pref.Name(name))
	if field == nil {
		oneOf := msg.Descriptor().Oneofs().ByName(pref.Name(name))
		if oneOf != nil {
			return pref.ValueOfMessage(msg), nil, oneOf, true
		}
		return pref.Value{}, nil, nil, false
	}
	return msg.Get(field), field, nil, true
}

// noRepeatField wraps the field descriptor to make a list/map field report as a scalar field.
type noRepeatField struct {
	pref.FieldDescriptor
}

func (f noRepeatField) Cardinality() pref.Cardinality {
	if f.FieldDescriptor.Cardinality() == pref.Repeated {
		return pref.Optional
	}
	return f.FieldDescriptor.Cardinality()
}

func (f noRepeatField) IsList() bool {
	return false
}

func (f noRepeatField) IsMap() bool {
	return false
}
