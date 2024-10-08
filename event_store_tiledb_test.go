package cc

import (
	"fmt"
	"os"
	"testing"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/usace/filesapi"
)

func TestTileDbStoreCreateArray(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.CreateArray(
		//creating a 10x10 array with a tile size of 5x5
		CreateArrayInput{
			ArrayPath: "dataset1",
			Attributes: []CcStoreAttr{
				{"position", ATTR_INT32},
				{"name", ATTR_STRING},
			},
			Dimensions: []Dimension{
				{
					"Y", //row
					DIMENSION_INT,
					[]int32{1, 10},
					5,
				},
				{
					"X", //col
					DIMENSION_INT,
					[]int32{1, 10},
					5,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreWriteArray(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	buffers := make([]WriteBuffer, 2)
	buffers[0] = WriteBuffer{
		AttrName: "position",
		Buffer:   []int32{1, 2, 3, 4},
	}

	buffers[1] = WriteBuffer{
		AttrName: "name",
		Buffer:   []byte("test1tester234test456test987"),
		Offsets:  []uint64{0, 5, 14, 21},
	}
	//buffers["Attr2"] = []int32{5, 6, 7, 8}
	//buffers["Attr2"] = [][]byte{[]byte("a1"), []byte("b2"), []byte("c3"), []byte("d4")}

	subarray := []int32{1, 2, 3, 4}
	input := WriteArrayInput{
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

func TestTileDbStoreGetArray(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath)
	if err != nil {
		t.Fatal(err)
	}

	input := ReadArrayInput{
		DataPath:    "dataset1",
		BufferRange: []int32{1, 2, 3, 4},
		AttrName:    "position",
	}

	err = eventStore.GetArray(input)
	if err != nil {
		t.Fatal(err)
	}

}

/*
func TestTileDbStoreWriteArray(t *testing.T) {
	//eventId := uuid.New()
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	data := []int32{102, 103, 104, 105, 106}
	input := WriteArrayInput{
		Buffer:    data,
		Size:      int64(len(data)),
		DataPath:  "test1/vals",
		ArrayType: ARRAY_DENSE,
	}

	err = eventStore.PutArray(input)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStoreReadArray(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	data := []int32{}
	input := ReadArrayInput{
		Buffer:   data,
		Size:     5,
		DataPath: "test1/vals",
	}

	err = eventStore.GetArray(input)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStorePutMetdataInt64Slice(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.PutMetadata("KEY2", []int{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStorePutMetdataInt32Slice(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	err = eventStore.PutMetadata("KEY3", []int32{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTileDbStorePutMetdataFloat64(t *testing.T) {
	eventId, _ := uuid.Parse("ff1dfb08-aed7-4f28-969b-a4829b77ee30")
	eventStore, err := NewTiledbEventStore(eventId)
	if err != nil {
		t.Fatal(err)
	}
	var val float64 = 123.456789
	err = eventStore.PutMetadata("KEY_FLOAT64", val)
	if err != nil {
		t.Fatal(err)
	}
}

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

	awsconfig := buildS3Config(CcProfile)
	if awscreds, ok := awsconfig.Credentials.(filesapi.S3FS_Static); ok {
		config.Set("vfs.s3.region", "us-east-1")
		config.Set("vfs.s3.aws_access_key_id", awscreds.S3Id)
		config.Set("vfs.s3.aws_secret_access_key", awscreds.S3Key)
	}

	rootPath := os.Getenv(CcRootPath)
	if rootPath == "" {
		rootPath = remoteRootPath //set to default
	}

	return tiledb.NewContext(config)

}

func TestCreateDenseArray(t *testing.T) {

	uri := "s3://cwbi-orm/tiledbtest/eventdb/dense1"

	ctx, err := getContext()

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rowDim, err := tiledb.NewDimension(ctx, "rows", tiledb.TILEDB_INT32, []int32{1, 4}, int32(4))
	if err != nil {
		t.Fatal(err)
	}
	colDim, err := tiledb.NewDimension(ctx, "cols", tiledb.TILEDB_INT32, []int32{1, 4}, int32(4))
	if err != nil {
		t.Fatal(err)
	}
	err = domain.AddDimensions(rowDim, colDim)
	if err != nil {
		t.Fatal(err)
	}

	// The array will be dense.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_DENSE)
	if err != nil {
		t.Fatal(err)
	}
	schema.SetDomain(domain)
	schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	if err != nil {
		t.Fatal(err)
	}
	schema.AddAttributes(a)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	err = array.Create(schema)
	if err != nil {
		t.Fatal(err)
	}

}

func TestWriteDenseArray(t *testing.T) {
	ctx, _ := getContext()
	uri := "s3://cwbi-orm/tiledbtest/eventdb/dense1"

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, _ := tiledb.NewArray(ctx, uri)
	array.Open(tiledb.TILEDB_WRITE)
	query, _ := tiledb.NewQuery(ctx, array)
	query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	query.SetDataBuffer("a", data)

	// Perform the write and close the array.
	query.Submit()
	array.Close()
}

func TestReadDenseArray(t *testing.T) {
	ctx, _ := getContext()
	uri := "s3://cwbi-orm/tiledbtest/eventdb/dense1"

	// Prepare the array for reading
	array, _ := tiledb.NewArray(ctx, uri)
	array.Open(tiledb.TILEDB_READ)

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 6)

	// Prepare the query
	query, _ := tiledb.NewQuery(ctx, array)
	query.SetSubArray(subArray)
	query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	query.SetDataBuffer("a", data)

	// Submit the query and close the array.
	query.Submit()
	array.Close()

	// Print out the results.
	fmt.Println(data)
}

func TestReadDenseArray2(t *testing.T) {
	ctx, _ := getContext()
	uri := "s3://cwbi-orm/tiledbtest/eventdb/dense1"

	// Prepare the array for reading
	array, _ := tiledb.NewArray(ctx, uri)
	array.Open(tiledb.TILEDB_READ)

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 1, 2, 4}

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 3)

	// Prepare the query
	query, _ := tiledb.NewQuery(ctx, array)
	query.SetSubArray(subArray)
	query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	query.SetDataBuffer("a", data)

	// Submit the query and close the array.
	query.Submit()
	array.Close()

	// Print out the results.
	fmt.Println(data)
}

// /////////////////////////////////////
// /////////////////////////////////////

func TestCreateMetadataArray(t *testing.T) {
	// Create a TileDB context.

	uri := "s3://cwbi-orm/tiledbtest/eventdb/scalars"

	ctx, err := getContext()
	if err != nil {
		t.Fatal(err)
	}

	domain, err := tiledb.NewDomain(ctx)
	if err != nil {
		t.Fatal(err)
	}

	dim, err := tiledb.NewDimension(ctx, "rows", tiledb.TILEDB_INT32, []int32{1, 1}, int32(1))
	if err != nil {
		t.Fatal("Error creating dimension:", err)
	}
	err = domain.AddDimensions(dim)
	if err != nil {
		t.Fatal(err)
	}

	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_DENSE)
	if err != nil {
		t.Fatal(err)
	}
	err = schema.SetDomain(domain)
	if err != nil {
		t.Fatal(err)
	}
	err = schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		t.Fatal(err)
	}
	err = schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		t.Fatal(err)
	}

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, _ := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	schema.AddAttributes(a)

	// Create the (empty) array on disk.
	array, _ := tiledb.NewArray(ctx, uri)
	array.Create(schema)
}

func TestWriteMetadata(t *testing.T) {
	ctx, _ := getContext()
	uri := "s3://cwbi-orm/tiledbtest/eventdb/scalars"
	array, err := tiledb.NewArray(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	err = array.Open(tiledb.TILEDB_WRITE)
	if err != nil {
		t.Fatal(err)
	}
	defer array.Close()

	err = array.PutMetadata("key1", int32(123))
	if err != nil {
		t.Fatal("Error setting metadata (int32):", err)
	}

	err = array.PutMetadata("key2", "hello TileDB")
	if err != nil {
		t.Fatal("Error setting metadata (string):", err)
	}
}

func TestReadMetadata(t *testing.T) {
	ctx, _ := getContext()
	uri := "s3://cwbi-orm/tiledbtest/eventdb/scalars"
	array, err := tiledb.NewArray(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	err = array.Open(tiledb.TILEDB_READ)
	if err != nil {
		t.Fatal(err)
	}
	defer array.Close()

	metadata, err := array.GetMetadataFromIndex(0)
	if err != nil {
		t.Fatal("Error getting metadata from index:", err)
	}
	fmt.Println(metadata.Key)
	fmt.Println(metadata.Value)

	mtype, vtype, val, err := array.GetMetadata("key2")
	if err != nil {
		t.Fatal("Error getting metadata from index:", err)
	}
	fmt.Println(mtype)
	fmt.Println(vtype)
	fmt.Println(val)
}
