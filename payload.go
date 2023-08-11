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
	i, err := GetAttribute[int64](p, name)
	return int(i), err
}

func (p PayloadAttributes) GetInt64(name string) (int64, error) {
	return GetAttribute[int64](p, name)
}

func (p PayloadAttributes) GetFloat(name string) (float64, error) {
	return GetAttribute[float64](p, name)
}

func (p PayloadAttributes) GetString(name string) (string, error) {
	return GetAttribute[string](p, name)
}

type PayloadAttributeTypes interface {
	int64 | int32 | float64 | string
}

func GetAttribute[T PayloadAttributeTypes](pa PayloadAttributes, name string) (T, error) {
	var t T
	if attr, ok := pa[name]; ok {
		tve := reflect.ValueOf(&t).Elem()
		tk := tve.Kind()
		switch tk {
		case reflect.Int32, reflect.Int64:
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
