package cc

import (
	"fmt"
	"reflect"
)

type ARRAY_TYPE int
type ATTR_TYPE int
type DIMENSION_TYPE int
type ARRAY_ORDER int

// used to get types for simple arrays.  disallow variable length types in simple arrays
var Golang2AttrTypeMap map[reflect.Kind]ATTR_TYPE = map[reflect.Kind]ATTR_TYPE{
	reflect.Float32: ATTR_FLOAT32,
	reflect.Float64: ATTR_FLOAT64,
	reflect.Uint8:   ATTR_UINT8,
	reflect.Int8:    ATTR_INT8,
	reflect.Int16:   ATTR_INT16,
	reflect.Int32:   ATTR_INT32,
	reflect.Int64:   ATTR_INT64,
	reflect.String:  ATTR_STRING,
}

const (
	DIMENSION_INT    DIMENSION_TYPE = 0
	DIMENSION_STRING DIMENSION_TYPE = 1

	ARRAY_DENSE  ARRAY_TYPE = 0 //default array type
	ARRAY_SPARSE ARRAY_TYPE = 1

	ARRAY_ORDER_ROWMAJOR  = 0 //default ordering
	ARRAY_ORDER_COLMAJOR  = 1
	ARRAY_ORDER_UNORDERED = 2

	ATTR_INT64   ATTR_TYPE = 0
	ATTR_INT32   ATTR_TYPE = 1
	ATTR_INT16   ATTR_TYPE = 2
	ATTR_INT8    ATTR_TYPE = 3
	ATTR_UINT8   ATTR_TYPE = 4
	ATTR_FLOAT32 ATTR_TYPE = 5
	ATTR_FLOAT64 ATTR_TYPE = 6
	ATTR_STRING  ATTR_TYPE = 7

	ATTR_STRUCT_TAG string = "eventstore"
)

type CcCache interface {
	PutArray()
	GetArray()
	DeleteArray()
	PutAttribute()
	GetAttribute()
	DeleteAttribute()
}

type MultiDimensionalArrayStore interface {
	CreateArray(input CreateArrayInput) error
	PutArray(input PutArrayInput) error
	GetArray(input GetArrayInput) (*ArrayResult, error)
	PutMetadata(key string, val any) error
	GetMetadata(key string, dest any) error
	DeleteMetadata(key string) error
}

type SimpleArrayStore interface {
	PutSimpleArray(input PutSimpleArrayInput) error
	GetSimpleArray(input GetSimpleArrayInput) (*ArrayResult, error)
}

/*
NOTES:
 - add gzip filters
*/

// MatrixTypeSupport
//type ArrayStoreDataType interface {
//	float32 | float64 | int64 | int32 | int16 | int8 | uint8 | string
//}

type CreateSimpleArrayInput struct {
	DataType ATTR_TYPE
	//represents the size of each dimension
	//there should be one value in the array for each dimension
	Dims      []int64
	ArrayPath string
}

type PutSimpleArrayInput struct {
	Buffer any

	//we insert the entire array on a simple array put
	//so the dims are the same as a create and represent the size of each dimension
	Dims     []int64
	DataPath string
}

type GetSimpleArrayInput struct {
	DataPath string
	XRange   []int64 //optional
	YRange   []int64 //optional
}

type CreateArrayInput struct {
	Attributes  []ArrayAttribute
	Dimensions  []ArrayDimension
	ArrayPath   string
	ArrayType   ARRAY_TYPE
	ArrayLayout ARRAY_ORDER
}

type ArrayAttribute struct {
	Name     string
	DataType ATTR_TYPE
}

type ArrayDimension struct {
	Name          string
	DimensionType DIMENSION_TYPE
	Domain        []int64
	TileExtent    int64
}

type PutArrayInput struct {
	Buffers     []PutArrayBuffer
	BufferRange []int64
	DataPath    string
	ArrayType   ARRAY_TYPE
	Coords      [][]int64
}

type PutArrayBuffer struct {
	AttrName string //attribute or domain name for the buffer
	Buffer   any
	Offsets  []uint64 //offsets for variable length data type buffers
}

type GetArrayInput struct {
	Attrs       []string
	DataPath    string
	BufferRange []int64
}

type ArraySchema struct {
	AttributeNames []string
	AttributeTypes []ATTR_TYPE
	Domain         []int64
	DomainNames    []string
	ArrayType      ARRAY_TYPE
}

func (as ArraySchema) GetType(attrname string) (ATTR_TYPE, error) {
	for i, v := range as.AttributeNames {
		if v == attrname {
			return as.AttributeTypes[i], nil
		}
	}
	return 0, fmt.Errorf("invalid attribute name: %s", attrname)
}

type ArrayResult struct {
	Range   []int64
	Data    []any
	Domains []any //exported for SPARSE array queries
	Schema  ArraySchema
	row     int
	Attrs   []string
	//Size   int
}

func (ar *ArrayResult) GetRow(rowindex int, attrindex int, dest any) {
	start := rowindex * int(ar.Range[3]-ar.Range[2]+1)
	end := start + int(ar.Range[3]-ar.Range[2]+1)
	v := reflect.ValueOf(ar.Data[attrindex])
	rowvals := v.Slice(start, end)
	reflect.ValueOf(dest).Elem().Set(rowvals)
}

// @TODO getcolumn and get row don't thow errors but can panic.
// Consider recovering from panics and returning errors
func (ar *ArrayResult) GetColumn(colindex int, attrindex int, dest any) {
	destType := reflect.TypeOf(dest).Elem()
	newVals := reflect.MakeSlice(destType, 0, 0)
	vals := reflect.ValueOf(ar.Data[attrindex])
	resultrows := ar.Range[1] - ar.Range[0] + 1
	resultcols := ar.Range[3] - ar.Range[2] + 1
	for i := 0; i < int(resultrows); i++ {
		index := (i * int(resultcols)) + colindex
		val := vals.Index(index)
		newVals = reflect.Append(newVals, val)
	}
	reflect.ValueOf(dest).Elem().Set(newVals)
}

func (ar *ArrayResult) Size() int {
	br := ar.Range
	var size int = 1
	for i := 0; i < len(br); i += 2 {
		size = size * (int(br[i+1]) - int(br[i]) + 1)
	}
	return size
}

func (ar *ArrayResult) Rows() int {
	return int(ar.Range[1] - ar.Range[0] + 1)
}

func (ar *ArrayResult) Cols() int {
	return int(ar.Range[3] - ar.Range[2] + 1)
}

func (ar *ArrayResult) Scan(val any) error {
	attrPosMap := tagAsPositionMap(ATTR_STRUCT_TAG, val)
	reflectVal := reflect.ValueOf(val)
	elemVal := reflectVal.Elem()
	for attr, pos := range attrPosMap {
		for i, s := range ar.Schema.AttributeNames {
			if s == attr {
				typ := ar.Schema.AttributeTypes[i]
				resultPosition := attrResultPosition(s, ar.Attrs)
				if resultPosition > -1 {
					val := handleType(ar.row, typ, ar.Data[resultPosition])
					field := elemVal.Field(pos)
					field.Set(reflect.ValueOf(val))
				}
			}
		}
	}
	ar.row++
	return nil
}

func attrResultPosition(attr string, attrs []string) int {
	for i, v := range attrs {
		if v == attr {
			return i
		}
	}
	return -1
}

func handleType(index int, attrType ATTR_TYPE, val any) any {
	vt := reflect.TypeOf(val)
	if vt.Kind() == reflect.Slice {
		vs := reflect.ValueOf(val)
		sval := vs.Index(index)
		if attrType == ATTR_STRING {
			return string(sval.Interface().([]uint8))
		}
		return sval.Interface()
	}
	return val
}

func tagAsPositionMap(tag string, data interface{}) map[string]int {
	tagmap := make(map[string]int)
	typ := reflect.TypeOf(data).Elem()
	fieldNum := typ.NumField()
	for i := 0; i < fieldNum; i++ {
		if tagval, ok := typ.Field(i).Tag.Lookup(tag); ok {
			tagmap[tagval] = i
		}
	}
	return tagmap
}
