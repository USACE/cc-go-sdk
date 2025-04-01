package cc

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/invopop/jsonschema"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cast"
)

type PayloadAttributes map[string]any

func (p PayloadAttributes) JSONSchemaExtend(schema *jsonschema.Schema) {
	schema.Type = "object"
	schema.AdditionalProperties = &jsonschema.Schema{
		Type: "string",
	}
}

func (p PayloadAttributes) GetInt(name string) (int, error) {
	return GetAttribute[int](p, name)
}

func (p PayloadAttributes) GetIntOrFail(name string) int {
	return GetOrFail[int](p, name)
}

func (p PayloadAttributes) GetIntOrDefault(name string, defaultValue int) int {
	return GetOrDefault[int](p, name, defaultValue)
}

func (p PayloadAttributes) GetIntSlice(name string) ([]int, error) {
	vals, ok := p[name]
	if !ok {
		return nil, fmt.Errorf("Invalid value for %s\n", name)
	}

	floatVals := Slice2Type[float64](vals.([]any))
	intvals := make([]int, len(floatVals))
	for i, f := range floatVals {
		intvals[i] = int(f)
	}
	return intvals, nil
}

func (p PayloadAttributes) GetInt64(name string) (int64, error) {
	return GetAttribute[int64](p, name)
}

func (p PayloadAttributes) GetInt64OrFail(name string) int64 {
	return GetOrFail[int64](p, name)
}

func (p PayloadAttributes) GetInt64OrDefault(name string, defaultValue int64) int64 {
	return GetOrDefault[int64](p, name, defaultValue)
}

func (p PayloadAttributes) GetFloat(name string) (float64, error) {
	return GetAttribute[float64](p, name)
}

func (p PayloadAttributes) GetFloatOrFail(name string) float64 {
	return GetOrFail[float64](p, name)
}

func (p PayloadAttributes) GetFloatOrDefault(name string, defaultValue float64) float64 {
	return GetOrDefault[float64](p, name, defaultValue)
}

func (p PayloadAttributes) GetFloatSlice(name string) ([]float64, error) {
	vals, ok := p[name]
	if !ok {
		return nil, fmt.Errorf("Invalid value for %s\n", name)
	}
	return Slice2Type[float64](vals.([]any)), nil
}

func (p PayloadAttributes) GetString(name string) (string, error) {
	return GetAttribute[string](p, name)
}

func (p PayloadAttributes) GetStringSlice(name string) ([]string, error) {
	vals, ok := p[name]
	if !ok {
		return nil, fmt.Errorf("Invalid value for %s\n", name)
	}
	return Slice2Type[string](vals.([]any)), nil
}

func (p PayloadAttributes) GetStringOrFail(name string) string {
	return GetOrFail[string](p, name)
}

func (p PayloadAttributes) GetStringOrDefault(name string, defaultVal string) string {
	return GetOrDefault[string](p, name, defaultVal)
}

func (p PayloadAttributes) GetBoolean(name string) (bool, error) {
	return GetAttribute[bool](p, name)
}

func (p PayloadAttributes) GetBooleanOrFail(name string) bool {
	return GetOrFail[bool](p, name)
}

func (p PayloadAttributes) GetBooleanOrDefault(name string, defaultVal bool) bool {
	return GetOrDefault[bool](p, name, defaultVal)
}

func (p PayloadAttributes) GetMap(name string) (map[string]any, error) {
	return GetAttribute[map[string]any](p, name)
}

func (p PayloadAttributes) Decode(name string, dest any) error {
	attrmap, err := GetAttribute[map[string]any](p, name)
	if err != nil {
		return err
	}
	return mapstructure.Decode(attrmap, &dest)
}

type PayloadAttributeTypes interface {
	int64 | int32 | int | float64 | string | bool | map[string]any
}

func GetOrFail[T PayloadAttributeTypes](pa PayloadAttributes, attr string) T {
	val, err := GetAttribute[T](pa, attr)
	if err != nil {
		log.Fatalf("Invalid value for %v\n", err)
	}
	return val
}

func GetOrDefault[T PayloadAttributeTypes](pa PayloadAttributes, attr string, defaultVal T) T {
	val, err := GetAttribute[T](pa, attr)
	if err != nil {
		val = defaultVal
		log.Printf("Invalid value for %v. Using default of: %v\n", err, defaultVal)
	}
	return val
}

func GetAttribute[T PayloadAttributeTypes](pa PayloadAttributes, name string) (T, error) {
	var t T
	if attr, ok := pa[name]; ok {
		tve := reflect.ValueOf(&t).Elem()
		tk := tve.Kind()
		switch tk {
		case reflect.Int64:
			i, err := cast.ToInt64E(attr)
			tve.Set(reflect.ValueOf(i))
			return t, err
		case reflect.Int:
			i, err := cast.ToInt64E(attr)
			tve.Set(reflect.ValueOf(int(i)))
			return t, err
		case reflect.Int32:
			i, err := cast.ToInt64E(attr)
			tve.Set(reflect.ValueOf(int32(i)))
			return t, err
		case reflect.String:
			s, err := cast.ToStringE(attr)
			tve.Set(reflect.ValueOf(s))
			return t, err
		case reflect.Bool:
			s, err := cast.ToStringE(attr)
			if err != nil {
				return t, err
			}
			val := (strings.ToLower(s) == "true")
			tve.Set(reflect.ValueOf(val))
			return t, err
		case reflect.Float64:
			f, err := cast.ToFloat64E(attr)
			tve.Set(reflect.ValueOf(f))
			return t, err
		case reflect.Map:
			i := cast.ToStringMap(attr)
			tve.Set(reflect.ValueOf(i))
			return t, nil
		default:
			return t, errors.New("Unsupported type for cast")
		}
	}
	return t, errors.New(fmt.Sprintf("Attribute %s is not in the payload\n", name))
}

func Slice2Type[T any](input []any) []T {
	out := make([]T, len(input))
	for i, v := range input {
		out[i] = v.(T)
	}
	return out
}
