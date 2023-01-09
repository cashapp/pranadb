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
	"github.com/squareup/pranadb/errors"

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
	Selector SelectorInjector
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

func (a *SelectorAST) ToSelector() SelectorInjector {
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

// SelectorInjector is a path selectorInjector/injector.
type SelectorInjector []Path

type Path struct {
	Field       *string
	NumberIndex *int
}

// ErrNotFound is returned when the selectorInjector references an invalid field
type ErrNotFound struct {
	missingPath SelectorInjector
	targetPath  SelectorInjector
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

// ParseColumnSelector parses a selectorInjector expression into an executable ColumnSelector.
func ParseColumnSelector(str string) (ColumnSelector, error) {
	s := &ColumnSelectorAST{}
	err := columnSelectorParser.ParseString("", str, s)
	return s.ToSelector(), errors.WithStack(err)
}

// ParseSelector parses a selectorInjector expression into an executable SelectorInjector.
func ParseSelector(str string) (SelectorInjector, error) {
	s := &SelectorAST{}
	err := selectorParser.ParseString("", str, s)
	return s.ToSelector(), errors.WithStack(err)
}

func (s SelectorInjector) expectScalar(orig SelectorInjector) error {
	if len(s) > 0 {
		return &ErrNotFound{missingPath: s, targetPath: orig}
	}
	return nil
}

func (s SelectorInjector) expectNonScalar(orig SelectorInjector) error {
	if len(s) == 0 {
		return errors.Errorf("selector %q does not point to a scalar value", orig)
	}
	return nil
}

func (s SelectorInjector) expectNonScalarString(orig SelectorInjector) error {
	if err := s.expectNonScalar(orig); err != nil {
		return err
	}
	if s[0].Field == nil {
		return errors.Errorf("selector %q should be a string in %q", s, orig)
	}
	return nil
}

func (s SelectorInjector) expectListIndex(orig SelectorInjector) error {
	if err := s.expectNonScalar(orig); err != nil {
		return err
	}
	if s[0].NumberIndex == nil {
		return errors.Errorf("cannot index list with string %q", *s[0].Field)
	}
	return nil
}

// Select evaluates the selectorInjector expression against the given value. The only supported types are
//     map[string]interface{}
//     []interface{}
//     google.golang.org/protobuf/reflect.Message
func (s SelectorInjector) Select(obj interface{}) (interface{}, error) {
	var ok bool
	for i, token := range s {
		switch vv := obj.(type) {
		case map[string]interface{}:
			if token.NumberIndex != nil {
				return nil, errors.Errorf("cannot use string to index map with number key at %q", s[0:i+1])
			}
			obj, ok = vv[*token.Field]
			if !ok {
				return nil, nil
			}
		case []interface{}:
			if token.Field != nil {
				return nil, errors.Errorf("cannot index array using %q at %q", *token.Field, s[0:i+1])
			}
			if len(vv) <= *token.NumberIndex {
				return nil, &ErrNotFound{s[0 : i+1], s}
			}
			obj = vv[*token.NumberIndex]
		case pref.Message:
			return s[i:].SelectProto(vv)
		}
	}
	return obj, nil
}

func (s SelectorInjector) Inject(obj interface{}, value interface{}) error {
	for i := 0; i < len(s); i++ {
		token := s[i]
		last := i == len(s)-1
		switch vv := obj.(type) {
		case map[string]interface{}:
			if token.NumberIndex != nil {
				return errors.Errorf("cannot use string to index map with number key at %q", s[0:i+1])
			}
			if !last {
				var ok bool
				var nestedObj interface{}
				nestedObj, ok = vv[*token.Field]
				if !ok {
					// Create the nested member
					nextToken := s[i+1]
					if nextToken.NumberIndex != nil {
						nestedObj = make([]interface{}, 1+*nextToken.NumberIndex)
					} else {
						nestedObj = make(map[string]interface{})
					}
					vv[*token.Field] = nestedObj
				}
				obj = nestedObj
			} else {
				vv[*token.Field] = value
			}
		case []interface{}:
			if token.Field != nil {
				return errors.Errorf("cannot index array using %q at %q", *token.Field, s[0:i+1])
			}
			if len(vv) <= *token.NumberIndex {
				return errors.NewPranaErrorf(errors.InvalidStatement, "Sink injectors that use array indexes must be declared in descending order of array index")
			}
			if !last {
				var nestedObj interface{}
				nestedObj = vv[*token.NumberIndex]
				if nestedObj == nil {
					nextToken := s[i+1]
					if nextToken.NumberIndex != nil {
						nestedObj = make([]interface{}, 1+*nextToken.NumberIndex)
					} else {
						nestedObj = make(map[string]interface{})
					}
					vv[*token.NumberIndex] = nestedObj
				}
				obj = nestedObj
			} else {
				vv[*token.NumberIndex] = value
			}
		case pref.Message:
			return s[i:].InjectProto(vv, value)
		}
	}
	return nil
}

func (s SelectorInjector) InjectProto(msg pref.Message, val interface{}) error {
	return s.traverseMessage(s, msg, val)
}

func (s SelectorInjector) traverseMessage(orig SelectorInjector, msg pref.Message, val interface{}) error {
	if err := s.expectNonScalarString(orig); err != nil {
		return err
	}
	tailSelectorInjector := s[1:]
	if field := msg.Descriptor().Fields().ByName(pref.Name(*s[0].Field)); field != nil {
		switch {
		case field.IsList():
			return tailSelectorInjector.traverseList(orig, msg.Mutable(field).List(), field, val)
		case field.IsMap():
			return tailSelectorInjector.traverseMap(orig, msg.Mutable(field).Map(), field, val)
		case field.Message() != nil:
			return tailSelectorInjector.traverseMessage(orig, msg.Mutable(field).Message(), val)
		default: // not a composite value
			return tailSelectorInjector.injectScalar(orig, msg, field, val)
		}
	}
	if oneOf := msg.Descriptor().Oneofs().ByName(pref.Name(*s[0].Field)); oneOf != nil {
		return tailSelectorInjector.traverseOneOf(orig, msg, oneOf, val)
	}
	return &ErrNotFound{missingPath: s, targetPath: orig}
}

func (s SelectorInjector) traverseOneOf(orig SelectorInjector, msg pref.Message, oneOf pref.OneofDescriptor, val interface{}) error {
	if err := s.expectNonScalarString(orig); err != nil {
		return err
	}
	tailSelectorInjector := s[1:]
	if field := oneOf.Fields().ByName(pref.Name(*s[0].Field)); field != nil {
		switch {
		case field.IsList():
			return tailSelectorInjector.traverseList(orig, msg.Mutable(field).List(), field, val)
		case field.IsMap():
			return tailSelectorInjector.traverseMap(orig, msg.Mutable(field).Map(), field, val)
		case field.Message() != nil:
			return tailSelectorInjector.traverseMessage(orig, msg.Mutable(field).Message(), val)
		default:
			return tailSelectorInjector.injectScalar(orig, msg, field, val)
		}
	}
	return nil
}

func (s SelectorInjector) traverseList(orig SelectorInjector, list pref.List, field pref.FieldDescriptor, val interface{}) error {
	if err := s.expectListIndex(orig); err != nil {
		return err
	}
	tailSelectorInjector := s[1:]
	idx := *s[0].NumberIndex
	if field.Message() != nil {
		for idx >= list.Len() {
			list.AppendMutable()
		}
		element := list.Get(idx)
		return tailSelectorInjector.traverseMessage(orig, element.Message(), val)
	}
	if err := tailSelectorInjector.expectScalar(orig); err != nil {
		return err
	}
	for idx >= list.Len() {
		list.Append(list.NewElement())
	}
	rVal, err := reflectValue(field, val)
	if err == nil {
		list.Set(idx, rVal)
	}
	return err
}

func (s SelectorInjector) traverseMap(orig SelectorInjector, m pref.Map, field pref.FieldDescriptor, val interface{}) error {
	if err := s.expectNonScalar(orig); err != nil {
		return err
	}
	tailSelectorInjector := s[1:]
	var key pref.MapKey
	switch {
	case s[0].Field != nil:
		key = pref.ValueOfString(*s[0].Field).MapKey()
	case s[0].NumberIndex != nil:
		key = pref.ValueOfInt64(int64(*s[0].NumberIndex)).MapKey()
	default:
		return errors.Error("invalid selector")
	}
	mapValueDescriptor := field.MapValue()
	if mapValueDescriptor.Message() != nil {
		mapValue := m.Get(key)
		if !mapValue.IsValid() {
			mapValue = m.NewValue()
			m.Set(key, mapValue)
		}
		return tailSelectorInjector.traverseMessage(orig, mapValue.Message(), val)
	}
	if err := tailSelectorInjector.expectScalar(orig); err != nil {
		return err
	}
	rVal, err := reflectValue(field.MapValue(), val)
	if err == nil {
		m.Set(key, rVal)
	}
	return err
}

func (s SelectorInjector) injectScalar(orig SelectorInjector, msg pref.Message, field pref.FieldDescriptor, val interface{}) error {
	if err := s.expectScalar(orig); err != nil {
		return err
	}
	rVal, err := reflectValue(field, val)
	if err == nil {
		msg.Set(field, rVal)
	}
	return err
}

func reflectValue(field pref.FieldDescriptor, val interface{}) (pref.Value, error) {
	if field.Enum() != nil {
		var enumValueDescriptor pref.EnumValueDescriptor
		switch vv := val.(type) {
		case string:
			enumValueDescriptor = field.Enum().Values().ByName(pref.Name(vv))
		case int:
			enumValueDescriptor = field.Enum().Values().ByNumber(pref.EnumNumber(vv))
		default:
			return pref.Value{}, errors.Error("enum value must be either int or string")
		}
		return pref.ValueOfEnum(enumValueDescriptor.Number()), nil
	}
	return pref.ValueOf(val), nil
}

// SelectProto returns the referenced value from the protobuf message. Adhering to Golang protobuf behavior, if a selectorInjector
// references nested value of a nil message, the default Go value will be returned. Array out of index will still panic.
// ErrNotFound is returned if a non-existing field is referenced. Other errors may be returned on failed type conversion.
// nolint: gocyclo
func (s SelectorInjector) SelectProto(msg pref.Message) (interface{}, error) {
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
				return nil, errors.Errorf("cannot get index %d of oneof field at %q", *token.NumberIndex, s[0:tail-1])
			}
			f = oneOf.Fields().ByName(pref.Name(*token.Field))
			if f == nil {
				return nil, errors.Errorf("unknown oneof field \"%s\"", *token.Field)
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
					return nil, errors.Errorf("cannot convert int to map key of kind %q at %q", f.MapKey().Kind(), s[0:tail-1])
				}
				if err != nil {
					return nil, errors.WithStack(err)
				}
				v = v.Map().Get(k)
				f = f.MapValue()
			default:
				return nil, errors.Errorf("cannot get index %d of %q", idx, f.Kind())
			}
		case token.Field != nil:
			fieldName := *token.Field
			switch {
			case f.IsMap():
				if f.MapKey().Kind() != pref.StringKind {
					return nil, errors.Errorf("cannot use string to index map with %q key", f.MapKey().Kind())
				}
				v = v.Map().Get(pref.ValueOfString(fieldName).MapKey())
				f = f.MapValue()
			case f.IsList():
				return nil, errors.Errorf("cannot index list at %q with string", s[0:tail])
			case f.Message() != nil:
				var ok bool
				v, f, oneOf, ok = getField(v.Message(), fieldName)
				if !ok {
					return nil, &ErrNotFound{missingPath: s[0:tail], targetPath: s}
				}
			default:
				return nil, errors.Errorf("cannot get field %q of %q", fieldName, f.Kind())
			}
		default:
			panic("invalid path token")
		}
	}
	if oneOf != nil {
		// When the selectorInjector terminates on a oneof field we return the name of the field as the value
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

func (s SelectorInjector) String() string {
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
