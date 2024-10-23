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
				{"attr1", ATTR_UINT8},
				{"attr2", ATTR_INT8},
				{"attr3", ATTR_INT16},
				{"attr4", ATTR_INT32},
				{"attr5", ATTR_INT64},
				{"attr6", ATTR_FLOAT32},
				{"attr7", ATTR_FLOAT64},
				{"attr8", ATTR_STRING},
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

	buffers := []WriteBuffer{
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
	eventStore, err := NewTiledbEventStore(eventPath)
	if err != nil {
		t.Fatal(err)
	}

	input := ReadArrayInput{
		DataPath:    "dataset1",
		BufferRange: []int32{1, 2, 3, 4},
		Attrs:       []string{"attr1", "attr2", "attr3", "attr4", "attr5", "attr6", "attr7", "attr8"},
	}

	result, err := eventStore.GetArray(input)
	if err != nil {
		t.Fatal(err)
	}
	ts := TestStruct{}
	for i := 0; i < result.Size; i++ {
		result.Scan(&ts)
		fmt.Println(ts)
	}
	fmt.Println(result)

}

func TestTileDbStorePutMetdataInt64Slice(t *testing.T) {
	eventPath := "sims/1"
	eventStore, err := NewTiledbEventStore(eventPath)
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
	eventStore, err := NewTiledbEventStore(eventPath)
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
	eventStore, err := NewTiledbEventStore(eventPath)
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
	eventStore, err := NewTiledbEventStore(eventPath)
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
	eventStore, err := NewTiledbEventStore(eventPath)
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
	eventStore, err := NewTiledbEventStore(eventPath)
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
