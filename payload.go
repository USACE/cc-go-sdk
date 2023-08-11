package cc

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/spf13/cast"
)

type Action struct {
	Name        string         `json:"name"`
	Type        string         `json:"type,omitempty"`
	Description string         `json:"desc"`
	Parameters  map[string]any `json:"params"`
}

type Payload struct {
	Attributes PayloadAttributes `json:"attributes,omitempty"`
	Stores     []DataStore       `json:"stores"`
	Inputs     []DataSource      `json:"inputs"`
	Outputs    []DataSource      `json:"outputs"`
	Actions    []Action          `json:"actions"`
}

type PayloadAttributes map[string]interface{}

func (p PayloadAttributes) GetIntAttr(name string) (int, error) {
	i, err := GetAttribute[int64](p, name)
	return int(i), err
}

func (p PayloadAttributes) GetInt64Attr(name string) (int64, error) {
	return GetAttribute[int64](p, name)
}

func (p PayloadAttributes) GetFloatAttr(name string) (float64, error) {
	return GetAttribute[float64](p, name)
}

func (p PayloadAttributes) GetStringAttr(name string) (string, error) {
	return GetAttribute[string](p, name)
}

type PayloadAttributeTypes interface {
	int64 | int32 | float64 | string
}

func GetAttribute[T PayloadAttributeTypes](pa PayloadAttributes, name string) (T, error) {
	var t T
	if attr, ok := pa[name]; ok {
		tv := reflect.ValueOf(t)
		tk := tv.Kind()
		switch tk {
		case reflect.Int32, reflect.Int64:
			i, err := cast.ToIntE(attr)
			tv.Set(reflect.ValueOf(i))
			return t, err
		case reflect.String:
			s, err := cast.ToStringE(attr)
			tv.Set(reflect.ValueOf(s))
			return t, err
		case reflect.Float64:
			f, err := cast.ToFloat64E(attr)
			tv.Set(reflect.ValueOf(f))
			return t, err
		default:
			return t, errors.New("Unsupported type for cast")
		}
	}
	return t, errors.New(fmt.Sprintf("Attribute %s is not in the payload\n", name))
}
