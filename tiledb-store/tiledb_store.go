package cc

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"

	. "github.com/usace/cc-go-sdk"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/usace/filesapi"
)

const (
	defaultAttrName     string = "a"
	defaultMetadataPath string = "/scalars"
	defaultTileExtent   int32  = 16
)

type TileDbEventStore struct {
	context *tiledb.Context
	uri     string
}

func NewTiledbEventStore(eventPath string) (*TileDbEventStore, error) {

	rootPath := os.Getenv(CcRootPath)
	if rootPath == "" {
		rootPath = RemoteRootPath //set to default
	}

	uri := fmt.Sprintf("s3://cwbi-orm%s/%s/eventdb", rootPath, eventPath)

	config, err := tiledb.NewConfig()
	if err != nil {
		return nil, err
	}

	awsconfig := BuildS3Config(CcProfile)
	if awscreds, ok := awsconfig.Credentials.(filesapi.S3FS_Static); ok {
		config.Set("vfs.s3.region", "us-east-1")
		config.Set("vfs.s3.aws_access_key_id", awscreds.S3Id)
		config.Set("vfs.s3.aws_secret_access_key", awscreds.S3Key)
	} else {
		return nil, errors.New("tiledb event store only supports static credentials")
	}

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

	arraySchema, err := tiledb.NewArraySchema(tdb.context, tiledb.TILEDB_DENSE)
	if err != nil {
		return err
	}

	if err = arraySchema.SetDomain(domain); err != nil {
		return err
	}
	if err = arraySchema.AddAttributes(tiledbAttrs...); err != nil {
		return err
	}
	arraySchema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	arraySchema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	//arraySchema.SetCellOrder(tiledb.TILEDB_COL_MAJOR)
	//arraySchema.SetTileOrder(tiledb.TILEDB_COL_MAJOR)

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
			fmt.Println(vr)
			data[i] = vr
		}
	}

	//size := bufferRangeSize(br)

	return &ArrayResult{
		Range:  br,
		Data:   data,
		Schema: schema,
		//Size:   size,
	}, nil
}

func GetSimpleArray() error {
	return nil
}

func (tdb *TileDbEventStore) createSimpleArray(input CreateSimpleArrayInput) error {
	xTileExtent := defaultTileExtent
	if input.XDim < xTileExtent {
		xTileExtent = input.XDim
	}
	yTileExtent := defaultTileExtent
	if input.YDim < yTileExtent {
		yTileExtent = input.YDim
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
			Dimensions: []ArrayDimension{
				{
					Name:          "Y",
					DimensionType: DIMENSION_INT,
					Domain:        []int32{1, input.YDim},
					TileExtent:    yTileExtent,
				},
				{
					Name:          "X",
					DimensionType: DIMENSION_INT,
					Domain:        []int32{1, input.XDim},
					TileExtent:    xTileExtent,
				},
			},
		},
	)
}

func (tdb *TileDbEventStore) PutSimpleArray(input PutSimpleArrayInput) error {
	object, err := tiledb.ObjectType(tdb.context, tdb.uri+"/"+input.DataPath)
	if err != nil {
		return err
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
				XDim:      input.XDim,
				YDim:      input.YDim,
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
			Buffer:   input.Buffer,
		},
	}
	br := []int32{1, input.YDim, 1, input.XDim}
	pinput := PutArrayInput{
		Buffers:     buffers,
		BufferRange: br,
		DataPath:    input.DataPath,
		ArrayType:   ARRAY_DENSE,
	}
	return tdb.PutArray(pinput)
}

func (tdb *TileDbEventStore) GetSimpleArray(input GetSimpleArrayInput) (*ArrayResult, error) {
	var bufferRange []int32
	if len(input.XRange) == 2 && len(input.YRange) == 2 {
		bufferRange = []int32{input.YRange[0], input.YRange[1], input.XRange[0], input.XRange[1]}
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
		return tiledb.TILEDB_INT32
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
			brange := make([]int32, ndim*2)
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
				if idomain, ok := domain.([]int32); ok {
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
