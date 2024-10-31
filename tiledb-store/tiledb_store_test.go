package cc

import (
	"fmt"
	"os"
	"testing"

	. "github.com/usace/cc-go-sdk"
	"github.com/usace/filesapi"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	//"github.com/usace/filesapi"
)

var testData []float64 = []float64{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
	30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
	40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
}

const (
	testProfile string = "FFRD"
)

//@TODO Test array layouts

// ////////SIMPLE ARRAY TESTS
func TestTileDbPutSimpleArray(t *testing.T) {
	//open store
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	//put simple array
	input := PutSimpleArrayInput{
		Buffer:   testData,
		DataPath: "five-by-ten-test",
		Dims:     []int64{5, 10}, //5 rows, 10 columns
	}

	err = eventStore.PutSimpleArray(input)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbGetSimpleArray(t *testing.T) {

	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	//Extract a 3 row by 5 column portion of the array
	input := GetSimpleArrayInput{
		DataPath: "five-by-ten-test",
		XRange:   []int64{5, 10},
		YRange:   []int64{2, 4},
	}

	result, err := eventStore.GetSimpleArray(input)
	if err != nil {
		t.Fatal(err)
	}

	//print values
	fmt.Println(result)
	fmt.Println(result.Rows())
	fmt.Println(result.Cols())

	//create a slice to hold row and column value arrays
	dest := []float64{}

	//enumerate rows and columns of the extracted data set
	//rows and column indices are relative to the result data, not the
	//full array
	for row := 0; row < result.Rows(); row++ {
		result.GetRow(row, 0, &dest)
		fmt.Println(dest)
	}

	for col := 0; col < result.Cols(); col++ {
		result.GetColumn(col, 0, &dest)
		fmt.Println(dest)
	}

	//extract the entire dataset
	//Ranges can be omitted
	input = GetSimpleArrayInput{
		DataPath: "five-by-ten-test",
	}

	result, err = eventStore.GetSimpleArray(input)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(result)
	fmt.Println(result.Rows())
	fmt.Println(result.Cols())

}

//////END SIMPLE ARRAY TESTS////

// ///////////////////////////
// //1D Dense Array Testing///
// //////////////////////////
func TestTileDbStoreCreate1dDenseArray(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.CreateArray(
		//creating a 10x10 array with a tile size of 5x5
		CreateArrayInput{
			ArrayPath: "dataset1",
			Attributes: []ArrayAttribute{
				{"attr1", ATTR_UINT8},
				{"attr2", ATTR_INT8},
				{"attr3", ATTR_INT16},
				{"attr4", ATTR_INT32},
				{"attr5", ATTR_INT64},
				{"attr6", ATTR_FLOAT32},
				{"attr7", ATTR_FLOAT64},
				{"attr8", ATTR_STRING},
			},
			Dimensions: []ArrayDimension{
				{
					Name:          "Y", //row
					DimensionType: DIMENSION_INT,
					Domain:        []int64{1, 10},
					TileExtent:    5,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreWrite1dDenseArray(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	buffers := []PutArrayBuffer{
		{
			AttrName: "attr1",
			Buffer:   []uint8{1, 2, 3, 4},
		},
		{
			AttrName: "attr2",
			Buffer:   []int8{5, 6, 7, 8},
		},
		{
			AttrName: "attr3",
			Buffer:   []int16{9, 10, 11, 12},
		},
		{
			AttrName: "attr4",
			Buffer:   []int32{13, 14, 15, 16},
		},
		{
			AttrName: "attr5",
			Buffer:   []int64{17, 18, 19, 20},
		},
		{
			AttrName: "attr6",
			Buffer:   []float32{1.1, 2.2, 3.3, 4.4},
		},
		{
			AttrName: "attr7",
			Buffer:   []float64{5.5, 6.6, 7.7, 8.8},
		},
		{
			AttrName: "attr8",
			Buffer:   []byte("test1tester234test456test987"),
			Offsets:  []uint64{0, 5, 14, 21},
		},
	}

	subarray := []int64{3, 6}
	input := PutArrayInput{
		Buffers:     buffers,
		BufferRange: subarray,
		DataPath:    "dataset1",
		ArrayType:   ARRAY_DENSE,
	}
	err = eventStore.PutArray(input)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreGet1dDenseArray(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	input := GetArrayInput{
		DataPath:    "dataset1",
		BufferRange: []int64{3, 6},
		Attrs:       []string{"attr1", "attr2", "attr3", "attr4", "attr5", "attr6", "attr7", "attr8"},
	}

	result, err := eventStore.GetArray(input)
	if err != nil {
		t.Fatal(err)
	}
	ts := TestStruct{}
	for i := 0; i < result.Size(); i++ {
		result.Scan(&ts)
		fmt.Println(ts)
	}
}

////END 1D Dense Array Testing///

// //////////////////////////////////////
// //n Dimensional Dense Array Testing///
// /////////////////////////////////////
func TestTileDbStoreCreateNdimDenseArray1(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.CreateArray(
		//creating a 4x4x4 array with a tile size of 2x2x2
		CreateArrayInput{
			ArrayPath: "ndimdense1",
			Attributes: []ArrayAttribute{
				{"attr1", ATTR_UINT8},
			},
			Dimensions: []ArrayDimension{
				{
					Name:          "d1",
					DimensionType: DIMENSION_INT,
					Domain:        []int64{1, 4},
					TileExtent:    2,
				},
				{
					Name:          "d2",
					DimensionType: DIMENSION_INT,
					Domain:        []int64{1, 4},
					TileExtent:    2,
				},
				{
					Name:          "d3",
					DimensionType: DIMENSION_INT,
					Domain:        []int64{1, 4},
					TileExtent:    2,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreWriteNdimDenseArray1(t *testing.T) {
	//create data to store
	data := make([]uint8, 64)
	for i := 0; i < len(data); i++ {
		data[i] = uint8(i)
	}
	//
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	buffers := []PutArrayBuffer{
		{
			AttrName: "attr1",
			Buffer:   data,
		},
	}

	input := PutArrayInput{
		Buffers:   buffers,
		DataPath:  "ndimdense1",
		ArrayType: ARRAY_DENSE,
	}
	err = eventStore.PutArray(input)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreGetNdimDenseArray1(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	input := GetArrayInput{
		DataPath: "ndimdense1",
		Attrs:    []string{"attr1"},
	}

	result, err := eventStore.GetArray(input)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(result.Data)
	ts := TestStruct{}
	for i := 0; i < result.Size(); i++ {
		result.Scan(&ts)
		fmt.Println(ts)
	}
}

func TestTileDbStoreWriteNdimDenseArray1b(t *testing.T) {
	//create data to store
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	buffers := []PutArrayBuffer{
		{
			AttrName: "attr1",
			Buffer:   []uint8{101, 102, 103, 104},
		},
	}

	subarray := []int64{3, 3, 1, 2, 2, 3}
	input := PutArrayInput{
		Buffers:     buffers,
		BufferRange: subarray,
		DataPath:    "ndimdense1",
		ArrayType:   ARRAY_DENSE,
	}
	err = eventStore.PutArray(input)
	if err != nil {
		t.Fatal(err)
	}
}

////END n Dimensional Dense Array Testing///

///////////////////////////////////////////
///////////Sparse Array Testing////////////
///////////////////////////////////////////

func TestTileDbCreateSparseArray(t *testing.T) {

	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.CreateArray(
		//creating a 4x4x4 array with a tile size of 2x2x2
		CreateArrayInput{
			ArrayPath: "sparse1",
			ArrayType: ARRAY_SPARSE,
			Attributes: []ArrayAttribute{
				{"attr1", ATTR_UINT8},
			},
			Dimensions: []ArrayDimension{
				{
					Name:          "d1",
					DimensionType: DIMENSION_INT,
					Domain:        []int64{1, 4},
					TileExtent:    2,
				},
				{
					Name:          "d2",
					DimensionType: DIMENSION_INT,
					Domain:        []int64{1, 4},
					TileExtent:    2,
				},
				{
					Name:          "d3",
					DimensionType: DIMENSION_INT,
					Domain:        []int64{1, 4},
					TileExtent:    2,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreWriteNdimSparseArray1b(t *testing.T) {
	//create data to store
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	buffers := []PutArrayBuffer{
		{
			AttrName: "attr1",
			Buffer:   []uint8{101, 102, 103, 104},
		},
		{
			AttrName: "d1",
			Buffer:   []int64{2, 2, 4, 4},
		},
		{
			AttrName: "d2",
			Buffer:   []int64{1, 2, 3, 4},
		},
		{
			AttrName: "d3",
			Buffer:   []int64{2, 3, 4, 4},
		},
	}

	input := PutArrayInput{
		Buffers:   buffers,
		DataPath:  "sparse1",
		ArrayType: ARRAY_SPARSE,
	}
	err = eventStore.PutArray(input)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreGetSparseArray1(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	input := GetArrayInput{
		DataPath: "sparse1",
		Attrs:    []string{"attr1"},
	}

	result, err := eventStore.GetArray(input)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(result.Data)
	ts := TestStruct{}
	fmt.Println(result.Size())
	for i := 0; i < result.Size(); i++ {
		result.Scan(&ts)
		fmt.Println(ts)
	}
}

func TestTileDbStoreGetSparseArray2(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	input := GetArrayInput{
		DataPath:    "sparse1",
		Attrs:       []string{"attr1"},
		BufferRange: []int64{2, 3, 1, 2, 1, 4},
	}

	result, err := eventStore.GetArray(input)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(result.Data)
	ts := TestStruct{}
	fmt.Println(result.Size())
	for i := 0; i < result.Size(); i++ {
		result.Scan(&ts)
		fmt.Println(ts)
	}
}

// //////////////////////////////////////
// //////OLD TESTS//////////////////////
// /////////////////////////////////////
func TestTiledbCreateSimpleArray(t *testing.T) {
	input := CreateSimpleArrayInput{
		DataType:  ATTR_FLOAT64,
		Dims:      []int64{5, 10},
		ArrayPath: "watersurface8",
	}
	eventPath := "sims/1/"

	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.CreateArray(
		//creating a 10x5 array with a tile size of 5x5
		CreateArrayInput{
			ArrayPath: input.ArrayPath,
			Attributes: []ArrayAttribute{
				{defaultAttrName, input.DataType},
			},
			Dimensions: []ArrayDimension{
				{
					"Y", //d1 corresponds to row domain
					DIMENSION_INT,
					[]int64{1, input.Dims[0]},
					5,
				},
				{
					"X", //col
					DIMENSION_INT,
					[]int64{1, input.Dims[1]},
					5,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTiledbWriteSimpleArray(t *testing.T) {
	input := PutSimpleArrayInput{
		Buffer:   testData,
		DataPath: "watersurface6",
	}

	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	buffers := []PutArrayBuffer{
		{
			AttrName: defaultAttrName,
			Buffer:   input.Buffer,
		},
	}
	br := []int64{1, 10, 1, 5}
	pinput := PutArrayInput{
		Buffers:     buffers,
		BufferRange: br,
		DataPath:    input.DataPath,
		ArrayType:   ARRAY_DENSE,
	}
	err = eventStore.PutArray(pinput)
	if err != nil {
		fmt.Println(err)
	}
}

func TestTileDbStoreWriteArray(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	buffers := []PutArrayBuffer{
		{
			AttrName: "attr1",
			Buffer:   []uint8{1, 2, 3, 4},
		},
		{
			AttrName: "attr2",
			Buffer:   []int8{5, 6, 7, 8},
		},
		{
			AttrName: "attr3",
			Buffer:   []int16{9, 10, 11, 12},
		},
		{
			AttrName: "attr4",
			Buffer:   []int32{13, 14, 15, 16},
		},
		{
			AttrName: "attr5",
			Buffer:   []int64{17, 18, 19, 20},
		},
		{
			AttrName: "attr6",
			Buffer:   []float32{1.1, 2.2, 3.3, 4.4},
		},
		{
			AttrName: "attr7",
			Buffer:   []float64{5.5, 6.6, 7.7, 8.8},
		},
		{
			AttrName: "attr8",
			Buffer:   []byte("test1tester234test456test987"),
			Offsets:  []uint64{0, 5, 14, 21},
		},
	}

	subarray := []int64{1, 2, 3, 4}
	input := PutArrayInput{
		Buffers:     buffers,
		BufferRange: subarray,
		DataPath:    "dataset1",
		ArrayType:   ARRAY_DENSE,
	}
	err = eventStore.PutArray(input)
	if err != nil {
		t.Fatal(err)
	}

}

type TestStruct struct {
	Val1 uint8   `eventstore:"attr1"`
	Val2 int8    `eventstore:"attr2"`
	Val3 int16   `eventstore:"attr3"`
	Val4 int32   `eventstore:"attr4"`
	Val5 int64   `eventstore:"attr5"`
	Val6 float32 `eventstore:"attr6"`
	Val7 float64 `eventstore:"attr7"`
	Val8 string  `eventstore:"attr8"`
}

func TestTileDbStoreGetArray(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}

	input := GetArrayInput{
		DataPath:    "dataset1",
		BufferRange: []int64{1, 2, 3, 4},
		Attrs:       []string{"attr1", "attr2", "attr3", "attr4", "attr5", "attr6", "attr7", "attr8"},
	}

	result, err := eventStore.GetArray(input)
	if err != nil {
		t.Fatal(err)
	}
	ts := TestStruct{}
	for i := 0; i < result.Size(); i++ {
		result.Scan(&ts)
		fmt.Println(ts)
	}
	fmt.Println(result)

}

func TestTileDbStorePutMetdataInt64Slice(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.PutMetadata("KEY_SLICE_INT64", []int{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStorePutMetdataInt32Slice(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.PutMetadata("KEY_SLICE_INT32", []int32{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStorePutMetdataFloat64(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	var val float64 = 123.456789
	err = eventStore.PutMetadata("KEY_FLOAT64", val)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreGetMetdata(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	var val float64
	err = eventStore.GetMetadata("KEY_FLOAT64", &val)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)
}

type MyStruct struct {
	Attr1 string
	Attr2 int64
}

func TestTileDbStorePutMetdataStruct(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	val := MyStruct{
		Attr1: "This is a test",
		Attr2: 9999,
	}

	err = eventStore.PutMetadata("KEY_STRUCT", val)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreDeleteMetdataFloat64(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath, testProfile)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.DeleteMetadata("KEY_FLOAT64")
	if err != nil {
		t.Fatal(err)
	}
}

/*


type MyStruct struct {
	Attr1 string
	Attr2 int64
}

func TestTileDbStorePutMetdataStruct(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	val := MyStruct{
		Attr1: "This is a test",
		Attr2: 9999,
	}

	err = eventStore.PutMetadata("KEY_STRUCT", val)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStorePutMetdataByteArray(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}

	val := []byte("this is a string")
	err = eventStore.PutMetadata("KEY_BYTES", val)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreGetMetdata(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	val, err := eventStore.GetMetadata("KEY2")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)
}

func TestTileDbStoreGetMetdata2(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	var val int
	err = eventStore.GetMetadata2("KEY_FLOAT64", &val)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)
}

func TestTileDbStoreGetMetdataBytes(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	val := []byte{}
	err = eventStore.GetMetadata2("KEY_BYTES", &val)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(val))
}
*/

/*
    if err := array.Open(tiledb.TILEDB_READ); err != nil {
        log.Fatal(err)
    }
    defer array.Close()
schema, err := array.Schema()
    if err != nil {
        log.Fatal(err)
    }

    // Get dimensions
    domain := schema.Domain()
    ndims := domain.NDim()
    fmt.Printf("Number of dimensions: %d\n", ndims)

    // Iterate over each dimension to get details
    for i := 0; i < ndims; i++ {
        dim := domain.Dimension(i)
        name := dim.Name()
        domainRange, _, _ := dim.Domain()
        fmt.Printf("Dimension %d: %s, Domain: [%v]\n", i+1, name, domainRange)
    }
*/

///////////////////////////////////////
//////////////////////////////////////

func getContext() (*tiledb.Context, error) {
	config, err := tiledb.NewConfig()
	if err != nil {
		return nil, err
	}

	awsconfig := BuildS3Config(CcProfile)
	if awscreds, ok := awsconfig.Credentials.(filesapi.S3FS_Static); ok {
		config.Set("vfs.s3.region", "us-east-1")
		config.Set("vfs.s3.aws_access_key_id", awscreds.S3Id)
		config.Set("vfs.s3.aws_secret_access_key", awscreds.S3Key)
	}

	rootPath := os.Getenv(CcRootPath)
	if rootPath == "" {
		rootPath = RemoteRootPath //set to default
	}

	return tiledb.NewContext(config)

}
