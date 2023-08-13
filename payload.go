package cc

import (
	"errors"
	"fmt"
	"log"
	"reflect"

	"github.com/spf13/cast"
)

type Action struct {
	Name        string            `json:"name"`
	Type        string            `json:"type,omitempty"`
	Description string            `json:"desc"`
	Parameters  PayloadAttributes `json:"params"`
}

type Payload struct {
	Attributes PayloadAttributes `json:"attributes,omitempty"`
	Stores     []DataStore       `json:"stores"`
	Inputs     []DataSource      `json:"inputs"`
	Outputs    []DataSource      `json:"outputs"`
	Actions    []Action          `json:"actions"`
}

type PayloadAttributes map[string]interface{}

func (p PayloadAttributes) GetInt(name string) (int, error) {
	return GetAttribute[int](p, name)
}

func (p PayloadAttributes) GetIntOrFail(name string) int {
	return GetOrFail[int](p, name)
}

func (p PayloadAttributes) GetIntOrDefault(name string, defaultValue int) int {
	return GetOrDefault[int](p, name, defaultValue)
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

func (p PayloadAttributes) GetString(name string) (string, error) {
	return GetAttribute[string](p, name)
}

func (p PayloadAttributes) GetStringOrFail(name string) string {
	return GetOrFail[string](p, name)
}

func (p PayloadAttributes) GetStringOrDefault(name string, defaultVal string) string {
	return GetOrDefault[string](p, name, defaultVal)
}

type PayloadAttributeTypes interface {
	int64 | int32 | int | float64 | string
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
		case reflect.Int, reflect.Int32, reflect.Int64:
			i, err := cast.ToInt64E(attr)
			tve.Set(reflect.ValueOf(i))
			return t, err
		case reflect.String:
			s, err := cast.ToStringE(attr)
			tve.Set(reflect.ValueOf(s))
			return t, err
		case reflect.Float64:
			f, err := cast.ToFloat64E(attr)
			tve.Set(reflect.ValueOf(f))
			return t, err
		default:
			return t, errors.New("Unsupported type for cast")
		}
	}
	return t, errors.New(fmt.Sprintf("Attribute %s is not in the payload\n", name))
}
