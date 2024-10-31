package cc

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"

	"github.com/usace/cc-go-sdk"
	. "github.com/usace/cc-go-sdk"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

const (
	defaultAttrName     string = "a"
	defaultMetadataPath string = "/scalars"
	defaultTileExtent   int64  = 16
)

type TileDbEventStore struct {
	context *tiledb.Context
	uri     string
}

var eventStoreType2TileDbType map[cc.ARRAY_TYPE]tiledb.ArrayType = map[cc.ARRAY_TYPE]tiledb.ArrayType{
	cc.ARRAY_DENSE:  tiledb.TILEDB_DENSE,
	cc.ARRAY_SPARSE: tiledb.TILEDB_SPARSE,
}

var eventStoreOrder2TileDbOrder map[cc.ARRAY_ORDER]tiledb.Layout = map[cc.ARRAY_ORDER]tiledb.Layout{
	cc.ARRAY_ORDER_ROWMAJOR:  tiledb.TILEDB_ROW_MAJOR,
	cc.ARRAY_ORDER_COLMAJOR:  tiledb.TILEDB_COL_MAJOR,
	cc.ARRAY_ORDER_UNORDERED: tiledb.TILEDB_UNORDERED,
}

func NewTiledbEventStore(eventPath string, profile string) (*TileDbEventStore, error) {

	rootPath := os.Getenv(CcRootPath)
	if rootPath == "" {
		rootPath = RemoteRootPath //set to default
	}

	S3Id := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsAccessKeyId))
	S3Key := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsSecretAccessKey))
	S3Region := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsDefaultRegion))
	S3Bucket := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsS3Bucket))

	uri := fmt.Sprintf("s3://%s/%s/eventdb", S3Bucket, rootPath)

	config, err := tiledb.NewConfig()
	if err != nil {
		return nil, err
	}

	//awsconfig := BuildS3Config(CcProfile)
	//if awscreds, ok := awsconfig.Credentials.(filesapi.S3FS_Static); ok {
	config.Set("vfs.s3.region", S3Region)
	config.Set("vfs.s3.aws_access_key_id", S3Id)
	config.Set("vfs.s3.aws_secret_access_key", S3Key)
	//} else {
	//	return nil, errors.New("tiledb event store only supports static credentials")
	//}

	context, err := tiledb.NewContext(config)
	if err != nil {
		return nil, err
	}

	store := TileDbEventStore{context, uri}
	err = store.createAttributeArray()
	return &store, err
}
func (tdb *TileDbEventStore) GetSession() any {
	return tdb.context
}

func (tdb *TileDbEventStore) Connect(ds DataStore) (any, error) {
	rootPath := os.Getenv(CcRootPath)
	if rootPath == "" {
		rootPath = RemoteRootPath //set to default
	}

	profile := ds.DsProfile
	S3Id := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsAccessKeyId))
	S3Key := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsSecretAccessKey))
	S3Region := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsDefaultRegion))
	S3Bucket := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsS3Bucket))

	//s3.us-gov-west-1.amazonaws.com

	uri := fmt.Sprintf("s3://%s/%s/eventdb", S3Bucket, rootPath)
	//uri := fmt.Sprintf("s3://%s%s/eventdb", bucket, rootPath)

	//uri := fmt.Sprintf("s3://cwbi-orm%s/%s/eventdb", rootPath, eventPath)

	config, err := tiledb.NewConfig()
	if err != nil {
		return nil, err
	}

	//awsconfig := BuildS3Config(CcProfile)
	//if awscreds, ok := awsconfig.Credentials.(filesapi.S3FS_Static); ok {
	config.Set("vfs.s3.region", S3Region)
	config.Set("vfs.s3.aws_access_key_id", S3Id)
	config.Set("vfs.s3.aws_secret_access_key", S3Key)
	//} else {
	//	return nil, errors.New("tiledb event store only supports static credentials")
	//}

	context, err := tiledb.NewContext(config)
	if err != nil {
		return nil, err
	}

	store := TileDbEventStore{context, uri}
	err = store.createAttributeArray()
	return &store, err
}

func (tdb *TileDbEventStore) CreateArray(input CreateArrayInput) error {
	domain, err := tiledb.NewDomain(tdb.context)
	if err != nil {
		return err
	}

	tiledbDims := make([]*tiledb.Dimension, len(input.Dimensions))
	for i, dimension := range input.Dimensions {
		var err error
		var dim *tiledb.Dimension
		switch dimension.DimensionType {
		case DIMENSION_STRING:
			dim, err = tiledb.NewStringDimension(tdb.context, dimension.Name)
		case DIMENSION_INT:
			dim, err = tiledb.NewDimension(
				tdb.context,
				dimension.Name,
				CcStoreDimensionType2TileDbType(dimension.DimensionType),
				dimension.Domain,
				dimension.TileExtent,
			)
		}
		if err != nil {
			return err
		}
		tiledbDims[i] = dim
	}

	err = domain.AddDimensions(tiledbDims...)
	if err != nil {
		return err
	}

	tiledbAttrs := make([]*tiledb.Attribute, len(input.Attributes))
	for i, attribute := range input.Attributes {
		if tiledbAttr, ok := ccAttr2TiledbAttrMap[attribute.DataType]; ok {
			tiledbAttrs[i], err = tiledb.NewAttribute(
				tdb.context,
				attribute.Name,
				tiledbAttr,
			)
			if err != nil {
				return err
			}

			if tiledbAttr == tiledb.TILEDB_STRING_ASCII {
				//err = tiledbAttrs[i].SetCellValNum(2)
				err = tiledbAttrs[i].SetCellValNum(tiledb.TILEDB_VAR_NUM)
				if err != nil {
					return err
				}
			}

		} else {
			return errors.New("unsupported attribute type")
		}
	}

	arraySchema, err := tiledb.NewArraySchema(tdb.context, eventStoreType2TileDbType[input.ArrayType])
	if err != nil {
		return err
	}

	if err = arraySchema.SetDomain(domain); err != nil {
		return err
	}
	if err = arraySchema.AddAttributes(tiledbAttrs...); err != nil {
		return err
	}

	//arraySchema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	//arraySchema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	arraySchema.SetCellOrder(eventStoreOrder2TileDbOrder[input.ArrayLayout])
	arraySchema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)

	array, err := tiledb.NewArray(tdb.context, tdb.uri+"/"+input.ArrayPath)
	if err != nil {
		return err
	}
	defer array.Close()

	return array.Create(arraySchema)
}

func (tdb *TileDbEventStore) PutArray(input PutArrayInput) error {
	array, err := tiledb.NewArray(tdb.context, tdb.uri+"/"+input.DataPath)
	if err != nil {
		return err
	}

	err = array.Open(tiledb.TILEDB_WRITE)
	if err != nil {
		return err
	}

	query, err := tiledb.NewQuery(tdb.context, array)
	if err != nil {
		return err
	}

	/////////////DENSE////////////////
	if err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR); err != nil {
		return err
	}

	for _, buffer := range input.Buffers {

		_, err = query.SetDataBuffer(buffer.AttrName, buffer.Buffer)
		if err != nil {
			return err
		}

		if buffer.Offsets != nil {
			_, err = query.SetOffsetsBuffer(buffer.AttrName, buffer.Offsets)
			if err != nil {
				return err
			}
		}

	}

	subarray, err := array.NewSubarray()
	if err != nil {
		return err
	}
	err = subarray.SetSubArray(input.BufferRange)
	if err != nil {
		return err
	}

	err = query.SetSubarray(subarray)
	if err != nil {
		return err
	}

	///////////////SPARSE//////////////////

	//if err = query.SetLayout(tiledb.TILEDB_UNORDERED); err != nil {
	//	return err
	//}

	//////////////////////////////////////

	err = query.Submit()
	if err != nil {
		return err
	}
	return nil
}

func (tdb *TileDbEventStore) GetArray(input GetArrayInput) (*ArrayResult, error) {
	array, err := tiledb.NewArray(tdb.context, tdb.uri+"/"+input.DataPath)
	if err != nil {
		return nil, err
	}

	err = array.Open(tiledb.TILEDB_READ)
	if err != nil {
		return nil, err
	}

	schema, err := getArraySchema(*array)
	if err != nil {
		return nil, err
	}

	query, err := tiledb.NewQuery(tdb.context, array)
	if err != nil {
		return nil, err
	}

	subarray, err := array.NewSubarray()
	if err != nil {
		return nil, err
	}

	br := input.BufferRange
	if len(br) == 0 {
		br = schema.Domain
	}

	err = subarray.SetSubArray(br)
	if err != nil {
		return nil, err
	}

	err = query.SetSubarray(subarray)
	if err != nil {
		return nil, err
	}

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		return nil, err
	}

	bufferElems, err := query.EstimateBufferElements()
	if err != nil {
		return nil, err
	}

	data := make([]any, len(input.Attrs))
	offsets := make([]*[]uint64, len(input.Attrs))
	for i, attr := range input.Attrs {
		attrtype, err := schema.GetType(attr)
		if err != nil {
			return nil, err
		}
		data[i], offsets[i] = createBuffer(attr, attrtype, bufferElems, query)
		_, err = query.SetDataBuffer(attr, data[i])
		if err != nil {
			return nil, err
		}
		if len(*offsets[i]) > 0 {
			_, err = query.SetOffsetsBuffer(attr, *offsets[i])
			if err != nil {
				return nil, err
			}
		}
	}

	err = query.Submit()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(data); i++ {
		if len(*offsets[i]) > 0 {
			vr := handleVariableResults(data[i].([]uint8), query, input.Attrs[i], *offsets[i])
			data[i] = vr
		}
	}

	//size := bufferRangeSize(br)

	return &ArrayResult{
		Range:  br,
		Data:   data,
		Schema: schema,
		Attrs:  input.Attrs,
		//Size:   size,
	}, nil
}

func GetSimpleArray() error {
	return nil
}

func (tdb *TileDbEventStore) createSimpleArray(input CreateSimpleArrayInput) error {
	tileExtent := defaultTileExtent
	for _, d := range input.Dims {
		if d < tileExtent {
			tileExtent = d
		}
	}
	dimensions := make([]ArrayDimension, len(input.Dims))
	for i := 0; i < len(input.Dims); i++ {
		dimensions[i] = ArrayDimension{
			Name:          strconv.Itoa(i),
			DimensionType: DIMENSION_INT,
			Domain:        []int64{1, input.Dims[i]},
			TileExtent:    tileExtent, //@TODO Fix this. Need to use a better tile extent
		}
	}
	return tdb.CreateArray(
		CreateArrayInput{
			ArrayPath: input.ArrayPath,
			Attributes: []ArrayAttribute{
				{
					Name:     defaultAttrName,
					DataType: input.DataType,
				},
			},
			Dimensions: dimensions,
		},
	)
}

func (tdb *TileDbEventStore) PutSimpleArray(input PutSimpleArrayInput) error {
	object, err := tiledb.ObjectType(tdb.context, tdb.uri+"/"+input.DataPath)
	if err != nil {
		return err
	}

	bufval := reflect.ValueOf(input.Buffer)
	if bufval.Kind() == reflect.Ptr {
		bufval = bufval.Elem() //dereference a buffer pointer reference
	}

	if object == tiledb.TILEDB_INVALID {
		//create array
		buftype := reflect.TypeOf(input.Buffer)
		if buftype.Kind() == reflect.Ptr {
			buftype = buftype.Elem()
		}

		if buftype.Kind() != reflect.Slice {
			return errors.New("invalid simple array type")
		}
		if newType, ok := Golang2AttrTypeMap[buftype.Elem().Kind()]; ok {
			err = tdb.createSimpleArray(CreateSimpleArrayInput{
				DataType:  newType,
				Dims:      input.Dims,
				ArrayPath: input.DataPath,
			})
			if err != nil {
				return err
			}
		} else {
			return errors.New("invalid simple array type")
		}
	}

	buffers := []PutArrayBuffer{
		{
			AttrName: defaultAttrName,
			Buffer:   bufval.Interface(),
		},
	}

	br := make([]int64, len(input.Dims)*2)
	for i := 0; i <= len(input.Dims); i += 2 {
		br[i] = 1
		br[i+1] = input.Dims[i/2]
	}

	pinput := PutArrayInput{
		Buffers:     buffers,
		BufferRange: br,
		DataPath:    input.DataPath,
		ArrayType:   ARRAY_DENSE,
	}
	return tdb.PutArray(pinput)
}

func (tdb *TileDbEventStore) GetSimpleArray(input GetSimpleArrayInput) (*ArrayResult, error) {
	var bufferRange []int64
	if len(input.XRange) == 2 && len(input.YRange) == 2 {
		bufferRange = []int64{input.YRange[0], input.YRange[1], input.XRange[0], input.XRange[1]}
	}
	ginput := GetArrayInput{
		Attrs:       []string{defaultAttrName},
		DataPath:    input.DataPath,
		BufferRange: bufferRange,
	}
	result, err := tdb.GetArray(ginput)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func handleVariableResults(data []uint8, query *tiledb.Query, attr string, offsets []uint64) [][]uint8 {
	elements, _ := query.ResultBufferElements()
	fmt.Println(elements)
	results := make([][]uint8, elements[attr][0])
	ranges := append(offsets, elements[attr][1])
	var dataPosition uint64 = 0
	for i := 0; i < len(results); i++ {
		size := ranges[i+1] - ranges[i]
		dataEnd := dataPosition + size
		variableVal := make([]byte, size)
		valPosition := 0
		for j := dataPosition; j < dataEnd; j++ {
			variableVal[valPosition] = data[dataPosition]
			dataPosition++
			valPosition++
		}
		results[i] = variableVal
	}
	return results
}

func createBuffer(attrName string, attrType ATTR_TYPE, bufferElems map[string][3]uint64, query *tiledb.Query) (any, *[]uint64) {
	var data any
	attrElem := bufferElems[attrName]
	var offsets []uint64
	if attrElem[0] == 0 {
		//fixed length
		switch attrType {
		case ATTR_UINT8:
			data = make([]uint8, attrElem[1])
		case ATTR_INT8:
			data = make([]int8, attrElem[1])
		case ATTR_INT16:
			data = make([]int16, attrElem[1])
		case ATTR_INT32:
			data = make([]int32, attrElem[1])
		case ATTR_INT64:
			data = make([]int64, attrElem[1])
		case ATTR_FLOAT32:
			data = make([]float32, attrElem[1])
		case ATTR_FLOAT64:
			data = make([]float64, attrElem[1])
		}

	} else {
		//variable length
		offsets = make([]uint64, attrElem[0])
		query.SetOffsetsBuffer(attrName, offsets) //@TODO echeck and handle or return error here
		data = make([]uint8, attrElem[0]*attrElem[1])
	}
	return data, &offsets
}

func CcStoreDimensionType2TileDbType(ccStoreDimType DIMENSION_TYPE) tiledb.Datatype {
	switch ccStoreDimType {
	case DIMENSION_INT:
		return tiledb.TILEDB_INT64
	}
	//default?
	return tiledb.TILEDB_INT32
}

var ccAttr2TiledbAttrMap map[ATTR_TYPE]tiledb.Datatype = map[ATTR_TYPE]tiledb.Datatype{
	ATTR_UINT8:   tiledb.TILEDB_UINT8,
	ATTR_INT8:    tiledb.TILEDB_INT8,
	ATTR_INT16:   tiledb.TILEDB_INT16,
	ATTR_INT32:   tiledb.TILEDB_INT32,
	ATTR_INT64:   tiledb.TILEDB_INT64,
	ATTR_FLOAT32: tiledb.TILEDB_FLOAT32,
	ATTR_FLOAT64: tiledb.TILEDB_FLOAT64,
	ATTR_STRING:  tiledb.TILEDB_STRING_ASCII,
}

func tileDbType2CcTypeLookup(value tiledb.Datatype) (ATTR_TYPE, bool) {
	for k, v := range ccAttr2TiledbAttrMap {
		if v == value {
			return k, true
		}
	}
	return -1, false
}

func getArraySchema(array tiledb.Array) (ArraySchema, error) {
	ccArraySchema := ArraySchema{}
	schema, err := array.Schema()
	if err != nil {
		return ccArraySchema, err
	}

	attributes, err := schema.Attributes()
	if err != nil {
		return ccArraySchema, err
	}

	names := make([]string, len(attributes))
	types := make([]ATTR_TYPE, len(attributes))
	for i, attr := range attributes {
		name, err := attr.Name()
		if err != nil {
			return ccArraySchema, err
		}
		names[i] = name

		typ, err := attr.Type()
		if err != nil {
			return ccArraySchema, err
		}
		if cctype, ok := tileDbType2CcTypeLookup(typ); ok {
			types[i] = cctype
		} else {
			return ccArraySchema, errors.New("Invalid CC Event Store Type")
		}

	}
	d, err := schema.Domain()
	//domain schema extraction is optional
	if err == nil {
		ndim, err := d.NDim()
		if err == nil {
			brange := make([]int64, ndim*2)
			for i := 0; i < int(ndim); i++ {
				dim, err := d.DimensionFromIndex(uint(i))
				if err != nil {
					log.Printf("Unable to extract array domain: %s\n", err)
					break
				}
				domain, err := dim.Domain()
				if err != nil {
					log.Printf("Unable to extract array domain: %s\n", err)
					break
				}
				if idomain, ok := domain.([]int64); ok {
					brange[2*i] = idomain[0]
					brange[2*i+1] = idomain[1]
				}
			}
			ccArraySchema.Domain = brange
		}
	}
	ccArraySchema.AttributeNames = names
	ccArraySchema.AttributeTypes = types

	return ccArraySchema, nil
}

/*
func GolangType2TileDbType(buf any) tiledb.Datatype {
	return tiledb.TILEDB_INT32
}
*/

/*
func ccStoreArrayType2Tiledbtype(ccArrayType ARRAY_TYPE) tiledb.ArrayType {
	return tiledb.TILEDB_DENSE
}
*/

func (tdb *TileDbEventStore) createAttributeArray() error {
	uri := tdb.uri + defaultMetadataPath

	objType, err := tiledb.ObjectType(tdb.context, uri)
	if err != nil {
		return err
	}
	if objType != tiledb.TILEDB_INVALID {
		//already created the array.
		return nil
	}

	//need to create the array...
	domain, err := tiledb.NewDomain(tdb.context)
	if err != nil {
		return err
	}

	dim, err := tiledb.NewDimension(tdb.context, "rows", tiledb.TILEDB_INT32, []int32{1, 1}, int32(1))
	if err != nil {
		return err
	}

	err = domain.AddDimensions(dim)
	if err != nil {
		return err
	}

	schema, err := tiledb.NewArraySchema(tdb.context, tiledb.TILEDB_DENSE)
	if err != nil {
		return err
	}

	err = schema.SetDomain(domain)
	if err != nil {
		return err
	}

	err = schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		return err
	}

	err = schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		return err
	}

	// Add a single default attribute so we can create the array to attach metadata to
	a, err := tiledb.NewAttribute(tdb.context, defaultAttrName, tiledb.TILEDB_INT32)
	if err != nil {
		return err
	}

	err = schema.AddAttributes(a)
	if err != nil {
		return err
	}

	array, err := tiledb.NewArray(tdb.context, uri)
	if err != nil {
		return err
	}
	defer array.Close()

	return array.Create(schema)
}

func (tdb *TileDbEventStore) PutMetadata(key string, val any) error {
	uri := tdb.uri + defaultMetadataPath
	array, err := tiledb.NewArray(tdb.context, uri)
	if err != nil {
		return err
	}
	err = array.Open(tiledb.TILEDB_WRITE)
	if err != nil {
		return err
	}
	defer array.Close()

	return array.PutMetadata(key, val)
}

func (tdb *TileDbEventStore) GetMetadata(key string, dest any) error {
	uri := tdb.uri + defaultMetadataPath
	array, err := tiledb.NewArray(tdb.context, uri)
	if err != nil {
		return err
	}
	err = array.Open(tiledb.TILEDB_READ)
	if err != nil {
		return err
	}
	defer array.Close()

	_, _, val, err := array.GetMetadata(key)
	if err != nil {
		return err
	}

	destTypePtr := reflect.TypeOf(dest) //dest type must be a pointer
	if destTypePtr.Kind() != reflect.Ptr {
		return errors.New("dest type must be a pointer")
	}

	destType := destTypePtr.Elem()
	valType := reflect.TypeOf(val)

	if destType != valType {
		return fmt.Errorf("dest type mismatch. expected %s got %s", destType.Name(), valType.Name())
	}

	reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(val))

	return err
}

func (tdb *TileDbEventStore) DeleteMetadata(key string) error {
	uri := tdb.uri + defaultMetadataPath
	array, err := tiledb.NewArray(tdb.context, uri)
	if err != nil {
		return err
	}
	err = array.Open(tiledb.TILEDB_WRITE)
	if err != nil {
		return err
	}
	defer array.Close()

	return array.DeleteMetadata(key)
}

////////////////////////////////////
//BufferData
////////////////////////////////////

func StructSliceToBuffers(data any) ([]BufferData, error) {
	dataType := reflect.TypeOf(data)
	dataVal := reflect.ValueOf(data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
		dataVal = dataVal.Elem()
	}

	bd, err := buildBuffers(dataType.Elem())
	if err != nil {
		log.Fatalln(err)
	}
	size := dataVal.Len()
	for i := 0; i < size; i++ {
		val := dataVal.Index(i)
		for j, attr := range bd {
			fmt.Println(attr.AttrName)
			field := val.Field(attr.StructPosition)
			bd[j].Buffer = reflect.Append(reflect.ValueOf(attr.Buffer), field).Interface()
			fmt.Println(field.Interface())
		}
	}
	bd.HandleStrings()
	return bd, nil
}

func buildBuffers(structType reflect.Type) (BufferDataSet, error) {
	tag := "eventstore"
	bd := []BufferData{} //BufferDataSet is an alias for []BufferData
	fieldNum := structType.NumField()
	for i := 0; i < fieldNum; i++ {
		field := structType.Field(i)
		if tagval, ok := field.Tag.Lookup(tag); ok {
			sliceType := reflect.SliceOf(field.Type)
			newSlice := reflect.MakeSlice(sliceType, 0, 0)
			if attrtype, ok := cc.Golang2AttrTypeMap[field.Type.Kind()]; ok {
				b := BufferData{
					AttrName:       tagval,
					StructPosition: i,
					AttrType:       attrtype,
					Buffer:         newSlice.Interface(),
				}
				bd = append(bd, b)
			} else {
				return nil, fmt.Errorf("unsupported golang type: %s", field.Type.Kind())
			}
		}
	}
	return bd, nil
}

type BufferData struct {
	AttrName       string
	StructPosition int
	AttrType       cc.ATTR_TYPE
	Buffer         any
	Offsets        []uint64
}

type BufferDataSet []BufferData

var MAXDIMENSION int32 = 2000000 //@TODO this is a bad idea

func (bd BufferDataSet) CreateArrayInput(arrayPath string) (CreateArrayInput, error) {
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

	input.Dimensions = []ArrayDimension{
		{
			Name:          "Y",
			DimensionType: DIMENSION_INT,
			Domain:        []int64{1, 327}, //@TODO.  Is this limited to 1D arrays, and how to i get siz4e.  Switch to sparse!
			TileExtent:    defaultTileExtent,
		},
	}

	return input, nil
}

func (bd BufferDataSet) PutArrayInput(arrayPath string) PutArrayInput {
	pabs := make([]PutArrayBuffer, len(bd))
	for i, buf := range bd {
		pabs[i] = PutArrayBuffer{
			AttrName: buf.AttrName,
			Buffer:   buf.Buffer,
			Offsets:  buf.Offsets,
		}
	}
	subarraySize := reflect.ValueOf(bd[1].Buffer).Len()
	return PutArrayInput{
		Buffers:     pabs,
		BufferRange: []int64{1, int64(subarraySize), 1, 1},
		DataPath:    arrayPath,
		ArrayType:   ARRAY_DENSE,
	}
}

func (bd BufferDataSet) HandleStrings() {
	for i, buf := range bd {
		if buf.AttrType == cc.ATTR_STRING {
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
