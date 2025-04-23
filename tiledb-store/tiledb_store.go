package cc

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"

	. "github.com/usace/cc-go-sdk"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

/*
@TODO: NOTES:add gzip filters
*/

const (
	defaultAttrName           string = "a"
	defaultMetadataPath       string = "/scalars"
	defaultTileExtent         int64  = 256
	stringSliceMetadataPrefix string = "__strslc_"
	stringSliceMetadataOffset string = "_offset_"
	stringSliceMetadataData   string = "_data_"
)

var webProtocolRegex *regexp.Regexp = regexp.MustCompile(`^(https?):\/\/(.*)$`)

type TileDbEventStore struct {
	context *tiledb.Context
	uri     string
}

var eventStoreType2TileDbType map[ARRAY_TYPE]tiledb.ArrayType = map[ARRAY_TYPE]tiledb.ArrayType{
	ARRAY_DENSE:  tiledb.TILEDB_DENSE,
	ARRAY_SPARSE: tiledb.TILEDB_SPARSE,
}

func getArrayType(at tiledb.ArrayType) (ARRAY_TYPE, error) {
	for k, v := range eventStoreType2TileDbType {
		if v == at {
			return k, nil
		}
	}
	return 0, fmt.Errorf("invalid array type: %v", at)
}

var eventStoreOrder2TileDbOrder map[LAYOUT_ORDER]tiledb.Layout = map[LAYOUT_ORDER]tiledb.Layout{
	ROWMAJOR:  tiledb.TILEDB_ROW_MAJOR,
	COLMAJOR:  tiledb.TILEDB_COL_MAJOR,
	UNORDERED: tiledb.TILEDB_UNORDERED,
}

func NewTiledbEventStore(eventPath string, profile string) (*TileDbEventStore, error) {
	store := TileDbEventStore{}
	_, err := store.Connect(DataStore{
		DsProfile: profile,
		Parameters: PayloadAttributes{
			"root": eventPath,
		},
	})
	return &store, err
}

func (tdb *TileDbEventStore) GetSession() any {
	return tdb.context
}

func (tdb *TileDbEventStore) Connect(ds DataStore) (any, error) {
	rootPath := ds.Parameters.GetStringOrFail("root")
	profile := ds.DsProfile
	S3Id := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsAccessKeyId))
	S3Key := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsSecretAccessKey))
	S3Region := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsDefaultRegion))
	S3Bucket := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsS3Bucket))
	S3Endpoint := os.Getenv(fmt.Sprintf("%s_%s", profile, AwsS3Endpoint))

	tdb.uri = fmt.Sprintf("s3://%s/%s/eventdb", S3Bucket, rootPath)
	config, err := tiledb.NewConfig()
	if err != nil {
		return nil, err
	}

	config.Set("vfs.s3.region", S3Region)
	config.Set("vfs.s3.aws_access_key_id", S3Id)
	config.Set("vfs.s3.aws_secret_access_key", S3Key)
	config.Set("vfs.s3.multipart_part_size", strconv.Itoa(5*1024*1024))
	config.Set("vfs.s3.max_parallel_ops", "2")
	if S3Endpoint != "" {
		match := webProtocolRegex.FindStringSubmatch(S3Endpoint)
		if len(match) == 3 {
			protocol := match[1]
			hostAndPath := match[2]
			config.Set("vfs.s3.scheme", protocol)
			config.Set("vfs.s3.endpoint_override", hostAndPath)
			config.Set("vfs.s3.use_virtual_addressing", "false")
		} else {
			log.Fatalln("Invalid S3Endpoint.  Endpoint must begin with the protocol: 'http://' or 'https://'.")
		}
	}

	context, err := tiledb.NewContext(config)
	if err != nil {
		return nil, err
	}

	tdb.context = context
	err = tdb.createAttributeArray()
	return tdb, err
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
				err = tiledbAttrs[i].SetCellValNum(tiledb.TILEDB_VAR_NUM)
				if err != nil {
					return err
				}
			}

		} else {
			return errors.New("unsupported attribute type")
		}
	}

	arrayType := eventStoreType2TileDbType[input.ArrayType]
	arraySchema, err := tiledb.NewArraySchema(tdb.context, arrayType)
	if err != nil {
		return err
	}

	if err = arraySchema.SetDomain(domain); err != nil {
		return err
	}
	if err = arraySchema.AddAttributes(tiledbAttrs...); err != nil {
		return err
	}

	celllayout := eventStoreOrder2TileDbOrder[input.CellLayout]
	tilelayout := eventStoreOrder2TileDbOrder[input.TileLayout]
	arraySchema.SetCellOrder(celllayout)
	arraySchema.SetTileOrder(tilelayout)

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
	if input.ArrayType == ARRAY_DENSE {
		querylayout := eventStoreOrder2TileDbOrder[input.PutLayout]
		if err = query.SetLayout(querylayout); err != nil {
			return err
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
	} else {

		///////////////SPARSE//////////////////

		if err = query.SetLayout(tiledb.TILEDB_UNORDERED); err != nil {
			return err
		}
	}

	//////////////////////////////////////

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

	br := getOpBufferRange(input.BufferRange, schema.Domain)

	err = subarray.SetSubArray(br)
	if err != nil {
		return nil, err
	}

	err = query.SetSubarray(subarray)
	if err != nil {
		return nil, err
	}

	searchlayout := eventStoreOrder2TileDbOrder[input.SearchOrder]
	err = query.SetLayout(searchlayout)
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
	//////////////////////////////
	//Set domains positions for sparse queries

	var domains []any
	if schema.ArrayType == ARRAY_SPARSE {
		domains = make([]any, len(schema.DomainNames))

		for i, domain := range schema.DomainNames {
			domainElem := bufferElems[domain]
			domains[i] = make([]int64, domainElem[1])
			_, err = query.SetDataBuffer(domain, domains[i])
			if err != nil {
				return nil, err
			}
		}
	}

	//////////////////////////////

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

	return &ArrayResult{
		Range:   br,
		Data:    data,
		Schema:  schema,
		Attrs:   input.Attrs,
		Domains: domains,
	}, nil
}

func getOpBufferRange(br []int64, domain []int64) []int64 {
	if len(br) == 0 {
		return domain
	}

	obr := make([]int64, len(br))
	for i := 0; i < len(br); i++ {
		if br[i] == 0 {
			obr[i] = domain[i]
		} else {
			obr[i] = br[i]
		}
	}
	return obr
}

func determineTileExtent(dims []int64) int64 {
	tileExtent := defaultTileExtent
	for _, dimSize := range dims {
		if dimSize < tileExtent {
			tileExtent = dimSize
		}
	}
	return tileExtent
}

func (tdb *TileDbEventStore) createSimpleArray(input CreateSimpleArrayInput) error {
	dimensions := make([]ArrayDimension, len(input.Dims))
	for i := 0; i < len(input.Dims); i++ {
		dimensions[i] = ArrayDimension{
			Name:          strconv.Itoa(i),
			DimensionType: DIMENSION_INT,
			Domain:        []int64{1, input.Dims[i]},
			TileExtent:    determineTileExtent(input.Dims), //@TODO Fix this. Need to use a better tile extent
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
			TileLayout: input.TileLayout,
			CellLayout: input.CellLayout,
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
				DataType:   newType,
				Dims:       input.Dims,
				ArrayPath:  input.DataPath,
				TileLayout: input.TileLayout,
				CellLayout: input.CellLayout,
				TileExtent: input.TileExtent,
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
		PutLayout:   input.PutLayout,
	}
	return tdb.PutArray(pinput)
}

func (tdb *TileDbEventStore) GetSimpleArray(input GetSimpleArrayInput) (*ArrayResult, error) {
	var bufferRange []int64
	if len(input.XRange) == 2 || len(input.YRange) == 2 {
		bufferRange = []int64{0, 0, 0, 0}
		if len(input.YRange) == 2 {
			bufferRange[0] = input.YRange[0]
			bufferRange[1] = input.YRange[1]
		}
		if len(input.XRange) == 2 {
			bufferRange[2] = input.XRange[0]
			bufferRange[3] = input.XRange[1]
		}
		//bufferRange = []int64{input.YRange[0], input.YRange[1], input.XRange[0], input.XRange[1]}
	}
	ginput := GetArrayInput{
		Attrs:       []string{defaultAttrName},
		DataPath:    input.DataPath,
		BufferRange: bufferRange,
		SearchOrder: input.SearchOrder,
	}
	result, err := tdb.GetArray(ginput)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func handleVariableResults(data []uint8, query *tiledb.Query, attr string, offsets []uint64) [][]uint8 {
	elements, _ := query.ResultBufferElements()
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

	tiledbArrayType, err := schema.Type()
	if err != nil {
		return ccArraySchema, err
	}
	ccArrayType, err := getArrayType(tiledbArrayType)
	if err != nil {
		return ccArraySchema, err
	}
	ccArraySchema.ArrayType = ccArrayType

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
			dnames := make([]string, ndim)
			for i := 0; i < int(ndim); i++ {
				dim, err := d.DimensionFromIndex(uint(i))
				if err != nil {
					log.Printf("Unable to extract array domain: %s\n", err)
					break
				}
				dname, err := dim.Name()
				if err != nil {
					log.Printf("Error extracting domain name: %s\n", err)
					break
				}
				dnames[i] = dname
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
			ccArraySchema.DomainNames = dnames
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

// This is the metadata array
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

	switch val := val.(type) {
	case []string:
		data, offsets := computeStringSliceMetadataComponents(val)
		offsetkey := fmt.Sprintf("%s%s%s", stringSliceMetadataPrefix, stringSliceMetadataOffset, key)
		datakey := fmt.Sprintf("%s%s%s", stringSliceMetadataPrefix, stringSliceMetadataData, key)
		err := array.PutMetadata(datakey, data)
		if err != nil {
			return err
		}
		return array.PutMetadata(offsetkey, offsets)
	default:
		return array.PutMetadata(key, val)
	}
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

	destTypePtr := reflect.TypeOf(dest) //dest type must be a pointer
	if destTypePtr.Kind() != reflect.Ptr {
		return errors.New("dest type must be a pointer")
	}

	switch dest := dest.(type) {
	case *[]string:
		offsetkey := fmt.Sprintf("%s%s%s", stringSliceMetadataPrefix, stringSliceMetadataOffset, key)
		datakey := fmt.Sprintf("%s%s%s", stringSliceMetadataPrefix, stringSliceMetadataData, key)

		_, _, dataval, err := array.GetMetadata(datakey)
		if err != nil {
			return err
		}

		_, _, offsetval, err := array.GetMetadata(offsetkey)
		if err != nil {
			return err
		}

		data, dataok := dataval.([]byte)
		offsets, offsetok := offsetval.([]int64)

		if dataok && offsetok {
			numvals := len(offsets)
			metadata := make([]string, numvals)
			offsets := append(offsets, int64(len(data)))
			for i := range numvals {
				metadata[i] = string(data[offsets[i]:offsets[i+1]])
			}
			reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(metadata))
			return nil
		} else {
			return fmt.Errorf("invalid offset or data types for %s", key)
		}
	default:
		_, _, val, err := array.GetMetadata(key)
		if err != nil {
			return err
		}

		valType := reflect.TypeOf(val)
		destTypePtr := reflect.TypeOf(dest) //dest type must be a pointer
		if destTypePtr.Kind() != reflect.Ptr {
			return errors.New("dest type must be a pointer")
		}
		destType := destTypePtr.Elem()

		if destType != valType {
			return fmt.Errorf("dest type mismatch. expected %s got %s", destType.Name(), valType.Name())
		}

		reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(val))

		return nil
	}
}

func (tdb *TileDbEventStore) GetMetadataOld(key string, dest any) error {

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

func computeStringSliceMetadataComponents(vals []string) ([]byte, []int64) {
	data := []byte{}
	position := 0
	offsets := make([]int64, len(vals))
	for i, v := range vals {
		bv := []byte(v)
		data = append(data, bv...)
		offsets[i] = int64(position)
		position = position + len(bv)
	}
	return data, offsets
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

///////
///////
//////
/*
func NewTiledbEventStore2(eventPath string, profile string) (*TileDbEventStore, error) {

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
*/
