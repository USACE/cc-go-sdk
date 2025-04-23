package cc

//event_store defines the interfaces and implements methods for supporting
//"cloud" native record and multidemesional chunked stores such as tiledb, zarr, and parquet
//due to the diverse nature of these stores, multiple semantics are defined:
// - Simple Array Store: This is a simple store for storing one or two dimesions arrays
//                 consisting of a single numeric value. For example grids or basic arrays
// - Multidimensional Array Stores: Chunked multidimensional arrays with any number of attributes
//                 at a given index
// - Record Store: A one dimensional array of records

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
)

type ARRAY_TYPE int
type ATTR_TYPE int
type DIMENSION_TYPE int
type LAYOUT_ORDER int

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

	ROWMAJOR  LAYOUT_ORDER = 0 //default layout order
	COLMAJOR  LAYOUT_ORDER = 1
	UNORDERED LAYOUT_ORDER = 2

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

var MAXDIMENSION int64 = 9223372036854775807 //@TODO this is probably a bad idea
var defaultTileExtent int64 = 256            //@TODO is this necessary?  It is repeated for TILEDB!

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

type MetadataStore interface {
	GetMetadata(key string, dest any) error
	PutMetadata(key string, val any) error
	DeleteMetadata(key string) error
}

type CreateSimpleArrayInput struct {
	DataType ATTR_TYPE
	//represents the size of each dimension
	//there should be one value in the array for each dimension
	Dims       []int64
	ArrayPath  string
	CellLayout LAYOUT_ORDER
	TileLayout LAYOUT_ORDER
	TileExtent []int64
}

type PutSimpleArrayInput struct {
	Buffer any

	//we insert the entire array on a simple array put
	//so the dims are the same as a create and represent the size of each dimension
	Dims       []int64
	DataPath   string
	CellLayout LAYOUT_ORDER
	TileLayout LAYOUT_ORDER
	PutLayout  LAYOUT_ORDER
	TileExtent []int64
}

type GetSimpleArrayInput struct {
	DataPath    string
	XRange      []int64      //optional
	YRange      []int64      //optional
	SearchOrder LAYOUT_ORDER //optional
}

type CreateArrayInput struct {
	Attributes []ArrayAttribute
	Dimensions []ArrayDimension
	ArrayPath  string
	ArrayType  ARRAY_TYPE
	CellLayout LAYOUT_ORDER
	TileLayout LAYOUT_ORDER
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
	PutLayout   LAYOUT_ORDER
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
	SearchOrder LAYOUT_ORDER
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
	//handle sparse array
	if len(ar.Domains) > 0 {
		return len(ar.Domains[0].([]int64))
	}

	//not a sparse array, calculate the size
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

////////////////////////////////////
//ArrayConfigData
////////////////////////////////////

func StructSliceToArrayConfig(data any) (ArrayAttrSet, error) {
	dataType := reflect.TypeOf(data)
	dataVal := reflect.ValueOf(data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
		dataVal = dataVal.Elem()
	}

	bd, err := buildBuffers(dataType.Elem())
	if err != nil {
		return bd, err
	}
	size := dataVal.Len()
	for i := 0; i < size; i++ {
		val := dataVal.Index(i)
		for j, attr := range bd {
			field := val.Field(attr.StructPosition)
			bd[j].Buffer = reflect.Append(reflect.ValueOf(attr.Buffer), field).Interface()
		}
	}
	bd.HandleStrings()
	return bd, nil
}

func buildBuffers(structType reflect.Type) (ArrayAttrSet, error) {
	tag := ATTR_STRUCT_TAG
	aad := []ArrayAttrData{}
	fieldNum := structType.NumField()
	for i := 0; i < fieldNum; i++ {
		field := structType.Field(i)
		if tagval, ok := field.Tag.Lookup(tag); ok {
			sliceType := reflect.SliceOf(field.Type)
			newSlice := reflect.MakeSlice(sliceType, 0, 0)
			if attrtype, ok := Golang2AttrTypeMap[field.Type.Kind()]; ok {
				b := ArrayAttrData{
					AttrName:       tagval,
					StructPosition: i,
					AttrType:       attrtype,
					Buffer:         newSlice.Interface(),
				}
				aad = append(aad, b)
			} else {
				return nil, fmt.Errorf("unsupported golang type: %s", field.Type.Kind())
			}
		}
	}
	return aad, nil
}

type ArrayAttrData struct {
	AttrName       string
	StructPosition int
	AttrType       ATTR_TYPE
	Buffer         any
	Offsets        []uint64
}

type ArrayAttrSet []ArrayAttrData

func getBufferLen(bd ArrayAttrData) int64 {
	if len(bd.Offsets) > 0 {
		return int64(len(bd.Offsets))
	}
	return int64(reflect.ValueOf(bd.Buffer).Len())
}

func (bd ArrayAttrSet) BuildCreateArrayInput(arrayPath string) (CreateArrayInput, error) {
	input := CreateArrayInput{}
	input.ArrayPath = arrayPath
	attributes := make([]ArrayAttribute, len(bd))
	for i, buf := range bd {
		attributes[i] = ArrayAttribute{
			Name:     buf.AttrName,
			DataType: buf.AttrType,
		}
	}
	input.Attributes = attributes

	//get length of buffer
	var size int64 = getBufferLen(bd[0])
	if size == 0 {
		size = MAXDIMENSION
	}

	tileExtent := defaultTileExtent
	if tileExtent > size {
		tileExtent = size
	}

	input.Dimensions = []ArrayDimension{
		{
			Name:          "d1",
			DimensionType: DIMENSION_INT,
			Domain:        []int64{1, size},
			TileExtent:    tileExtent,
		},
	}

	return input, nil
}

func (bd ArrayAttrSet) BuildPutArrayInput(arrayPath string, arrayType ARRAY_TYPE) PutArrayInput {

	pabs := make([]PutArrayBuffer, len(bd))
	for i, buf := range bd {
		pabs[i] = PutArrayBuffer{
			AttrName: buf.AttrName,
			Buffer:   buf.Buffer,
			Offsets:  buf.Offsets,
		}
	}

	bufSize := getBufferLen(bd[0])

	if arrayType == ARRAY_SPARSE {
		dbuf := make([]int64, bufSize)
		for i := 1; i <= int(bufSize); i++ {
			dbuf[i-1] = int64(i)
		}
		pabs = append(pabs, PutArrayBuffer{
			AttrName: "d1",
			Buffer:   dbuf,
		})
	}

	return PutArrayInput{
		Buffers:     pabs,
		BufferRange: []int64{1, bufSize},
		DataPath:    arrayPath,
		ArrayType:   arrayType,
	}
}

func (bd ArrayAttrSet) AttributNames() []string {
	attributes := make([]string, len(bd))
	for i, buf := range bd {
		attributes[i] = buf.AttrName
	}
	return attributes
}

func (bd ArrayAttrSet) HandleStrings() {
	for i, buf := range bd {
		if buf.AttrType == ATTR_STRING {
			oldSlice := reflect.ValueOf(buf.Buffer)
			sliceLen := oldSlice.Len()
			var newBuff bytes.Buffer
			offsets := make([]uint64, sliceLen)
			offsetIndex := 0
			for j := 0; j < sliceLen; j++ {
				oldVal := oldSlice.Index(j)
				newBytes := []byte(oldVal.String())
				newBuff.Write(newBytes)
				offsets[j] = uint64(offsetIndex)
				offsetIndex += len(newBytes)
			}
			bd[i].Buffer = newBuff.Bytes()
			bd[i].Offsets = offsets
		}
	}
}

type Recordset struct {
	buffer    any
	bds       ArrayAttrSet
	storename string
	datapath  string
	//indexfield string
	pm *PluginManager

	ArrayType  ARRAY_TYPE   //optional: default is dense array
	ArrayOrder LAYOUT_ORDER //optional: default is row ordering
}

// buffer is a pointer to a slice of a struct
func NewEventStoreRecordset(pm *PluginManager, buffer any, storename string, datapath string) (*Recordset, error) {
	buffData, err := StructSliceToArrayConfig(buffer)
	if err != nil {
		return nil, err
	}
	return &Recordset{
		pm:        pm,
		buffer:    buffer,
		bds:       buffData,
		datapath:  datapath,
		storename: storename,
		//indexfield: indexfield,
	}, nil
}

func (rs *Recordset) Create() error {
	tdb, err := rs.pm.GetStore(rs.storename)
	if err != nil {
		return err
	}
	if mds, ok := tdb.Session.(MultiDimensionalArrayStore); ok {
		input, _ := rs.bds.BuildCreateArrayInput(rs.datapath)
		return mds.CreateArray(input)
	}
	return errors.New("store does not support multi dimesional arrays")
}

func (rs *Recordset) Write(buff any) error {
	tdb, err := rs.pm.GetStore(rs.storename)
	if err != nil {
		return err
	}
	if mds, ok := tdb.Session.(MultiDimensionalArrayStore); ok {
		input := rs.bds.BuildPutArrayInput(rs.datapath, ARRAY_DENSE) //@TODO check array type for all BuildPutArray
		return mds.PutArray(input)
	}
	return errors.New("store does not support multi dimesional arrays")
}

func (rs *Recordset) Read(recrange ...int64) (*ArrayResult, error) {
	bufferRange := []int64{}
	if len(recrange) == 2 {
		bufferRange = []int64{recrange[0], recrange[1]}
	}
	tdb, err := rs.pm.GetStore(rs.storename)
	if err != nil {
		return nil, err
	}
	if mds, ok := tdb.Session.(MultiDimensionalArrayStore); ok {
		input := GetArrayInput{
			DataPath:    rs.datapath,
			BufferRange: bufferRange,
			Attrs:       rs.bds.AttributNames(),
		}

		return mds.GetArray(input)
	}
	return nil, errors.New("store does not support multi dimesional arrays")
}
