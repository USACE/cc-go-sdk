package cc

import (
	"errors"
	"fmt"
	"os"
	"reflect"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/usace/filesapi"
)

const (
	defaultAttrName     string = "a"
	defaultMetadataPath string = "/scalars"
)

type TileDbEventStore struct {
	context *tiledb.Context
	uri     string
}

func NewTiledbEventStore(eventPath string) (CcEventStore, error) {

	rootPath := os.Getenv(CcRootPath)
	if rootPath == "" {
		rootPath = remoteRootPath //set to default
	}

	uri := fmt.Sprintf("s3://cwbi-orm%s/%s/eventdb", rootPath, eventPath)

	config, err := tiledb.NewConfig()
	if err != nil {
		return nil, err
	}

	awsconfig := buildS3Config(CcProfile)
	if awscreds, ok := awsconfig.Credentials.(filesapi.S3FS_Static); ok {
		config.Set("vfs.s3.region", "us-east-1")
		config.Set("vfs.s3.aws_access_key_id", awscreds.S3Id)
		config.Set("vfs.s3.aws_secret_access_key", awscreds.S3Key)
	} else {
		return nil, errors.New("Tiledb Event Store only supports Static Credentials")
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
			return errors.New("Unsupported attribute type")
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

	array, err := tiledb.NewArray(tdb.context, tdb.uri+"/"+input.ArrayPath)
	if err != nil {
		return err
	}
	defer array.Close()

	return array.Create(arraySchema)
}

func (tdb *TileDbEventStore) PutArray(input WriteArrayInput) error {
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
		if buffer.Offsets != nil {
			_, _, err = query.SetBufferVar(buffer.AttrName, buffer.Offsets, buffer.Buffer)
			if err != nil {
				return err
			}
		} else {
			_, err = query.SetDataBuffer(buffer.AttrName, buffer.Buffer)
			if err != nil {
				return err
			}
		}

		/*
			if buffer.Offsets != nil {
				query.SetOffsetsBuffer(buffer.AttrName, buffer.Offsets)
			}
		*/

	}

	err = query.SetSubArray(input.BufferRange)

	err = query.Submit()
	if err != nil {
		return err
	}
	return nil
}

func (tdb *TileDbEventStore) GetArrayOld(input ReadArrayInput) error {
	array, err := tiledb.NewArray(tdb.context, tdb.uri+"/"+input.DataPath)
	if err != nil {
		return err
	}

	err = array.Open(tiledb.TILEDB_READ)
	if err != nil {
		return err
	}

	query, err := tiledb.NewQuery(tdb.context, array)
	if err != nil {
		return err
	}

	err = query.SetSubArray(input.BufferRange)
	if err != nil {
		return err
	}

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		return err
	}

	bufferElems, _ := query.EstimateBufferElements()
	offsets := make([]uint64, bufferElems[input.AttrName][0])
	query.SetOffsetsBuffer(input.AttrName, offsets)

	data := make([]uint8, 32)
	_, err = query.SetDataBuffer(input.AttrName, data)
	if err != nil {
		return err
	}

	data2 := make([]int32, 4)
	_, err = query.SetDataBuffer("Attr1", data2)
	if err != nil {
		return err
	}

	// Submit the query and close the array.
	err = query.Submit()
	if err != nil {
		return err
	}

	elements, _ := query.ResultBufferElements()
	fmt.Println(elements)
	results := make([][]uint8, elements[input.AttrName][0])
	ranges := append(offsets, elements[input.AttrName][1])
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
	array.Close()
	for _, result := range results {
		fmt.Println(string(result))
	}
	fmt.Println(string(data))
	fmt.Println(data2)
	return nil

	////////////

}

func (tdb *TileDbEventStore) GetArray(input ReadArrayInput) error {
	array, err := tiledb.NewArray(tdb.context, tdb.uri+"/"+input.DataPath)
	if err != nil {
		return err
	}

	err = array.Open(tiledb.TILEDB_READ)
	if err != nil {
		return err
	}

	schema, err := array.Schema()
	fmt.Println(schema)

	query, err := tiledb.NewQuery(tdb.context, array)
	if err != nil {
		return err
	}

	err = query.SetSubArray(input.BufferRange)
	if err != nil {
		return err
	}

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		return err
	}

	bufferElems, err := query.EstimateBufferElements()
	if err != nil {
		return err
	}

	data, offsets := createBuffer("Attr2", bufferElems, query)
	_, err = query.SetDataBuffer(input.AttrName, data)
	if err != nil {
		return err
	}

	data2 := make([]int32, 4)
	_, err = query.SetDataBuffer("Attr1", data2)
	if err != nil {
		return err
	}

	// Submit the query and close the array.
	err = query.Submit()
	if err != nil {
		return err
	}

	results := handleVariableResults(data.([]uint8), query, input, *offsets)
	array.Close()
	for _, result := range results {
		fmt.Println(string(result))
	}
	fmt.Println(data2)
	return nil

}

func handleVariableResults(data []uint8, query *tiledb.Query, input ReadArrayInput, offsets []uint64) [][]uint8 {
	elements, _ := query.ResultBufferElements()
	fmt.Println(elements)
	results := make([][]uint8, elements[input.AttrName][0])
	ranges := append(offsets, elements[input.AttrName][1])
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

func createBuffer(attrName string, bufferElems map[string][3]uint64, query *tiledb.Query) (any, *[]uint64) {
	var data any
	attrElem := bufferElems[attrName]
	var offsets []uint64
	if attrElem[0] == 0 {
		//fixed length
		data = make([]int32, attrElem[1])
	} else {
		//variable length
		offsets = make([]uint64, bufferElems[attrName][0])
		query.SetOffsetsBuffer(attrName, offsets) //@TODO echeck and handle or return error here
		data = make([]uint8, attrElem[0]*attrElem[1])
	}
	return data, &offsets
}

/*
func (tdb *TileDbEventStore) PutMatrix(input WriteMatrixInput) error {

}

func (tdb *TileDbEventStore) PutArray2(input WriteArrayInput) error {
	domain, err := tiledb.NewDomain(tdb.context)
	if err != nil {
		return err
	}
	tiledbtype := GolangType2TileDbType(input.Buffer)


	rowdim, err := tiledb.NewDimension(tdb.context, "rows", tiledbtype, []int32{1, 1}, int32(1))
	if err != nil {
		return err
	}
	coldim, err := tiledb.NewDimension(tdb.context, "cols", tiledbtype, []int32{1, int32(input.Size)}, int32(input.Size))
	if err != nil {
		return err
	}

	err = domain.AddDimensions(rowdim, coldim)
	if err != nil {
		return err
	}

	//we are not allowing attribute name manipulation, so it will be set to a default constant
	attribute, err := tiledb.NewAttribute(tdb.context, defaultAttrName, tiledbtype)
	if err != nil {
		return err
	}

	arraySchema, err := tiledb.NewArraySchema(tdb.context, tiledb.TILEDB_DENSE)
	if err != nil {
		return err
	}

	if err = arraySchema.SetDomain(domain); err != nil {
		return err
	}
	if err = arraySchema.AddAttributes(attribute); err != nil {
		return err
	}

	array, err := tiledb.NewArray(tdb.context, tdb.uri+"/"+input.DataPath)
	if err != nil {
		return err
	}
	defer array.Close()

	err = array.Create(arraySchema)
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

	_, err = query.SetDataBuffer(defaultAttrName, input.Buffer)
	if err != nil {
		return err
	}

	err = query.Submit()
	if err != nil {
		return err
	}
	return nil
}

func (tdb *TileDbEventStore) GetArray(input ReadArrayInput) error {

	array, err := tiledb.NewArray(tdb.context, tdb.uri+"/"+input.DataPath)
	if err != nil {
		return err
	}

	err = array.Open(tiledb.TILEDB_READ)
	if err != nil {
		return err
	}

	// Prepare the query
	query, err := tiledb.NewQuery(tdb.context, array)
	if err != nil {
		return err
	}
	//query.SetSubArray(subArray)
	//subArray := []int32{1, 5, 1, 1}
	//subArray := []int32{1, 1, 1, 5}
	subArray := []int32{1, 1, 1, int32(input.Size)}
	data := make([]int32, input.Size)

	err = query.SetSubArray(subArray)
	if err != nil {
		return err
	}

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer(defaultAttrName, data)
	if err != nil {
		return err
	}

	// Submit the query and close the array.
	err = query.Submit()
	if err != nil {
		return err
	}

	array.Close()

	// Print out the results.
	fmt.Println(data)
	return nil
}
*/

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

func GolangType2TileDbType(buf any) tiledb.Datatype {
	return tiledb.TILEDB_INT32
}

func ccStoreArrayType2Tiledbtype(ccArrayType ARRAY_TYPE) tiledb.ArrayType {
	return tiledb.TILEDB_DENSE
}

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

func (tdb *TileDbEventStore) GetMetadata(key string) (any, error) {
	uri := tdb.uri + defaultMetadataPath
	array, err := tiledb.NewArray(tdb.context, uri)
	if err != nil {
		return nil, err
	}
	err = array.Open(tiledb.TILEDB_READ)
	if err != nil {
		return nil, err
	}
	defer array.Close()

	_, _, val, err := array.GetMetadata(key)

	return val, err
}

func (tdb *TileDbEventStore) GetMetadata2(key string, dest any) error {
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

	destTypePtr := reflect.TypeOf(dest) //dest type must be a pointer
	if destTypePtr.Kind() != reflect.Ptr {
		return errors.New("Dest type must be a pointer")
	}

	destType := destTypePtr.Elem()
	valType := reflect.TypeOf(val)

	if destType != valType {
		return fmt.Errorf("Dest type mismatch. Expected %s got %s\n", destType.Name(), valType.Name())
	}

	reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(val))

	return err
}
