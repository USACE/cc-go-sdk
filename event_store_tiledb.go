package cc

import (
	"errors"
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/google/uuid"
	"github.com/usace/filesapi"
)

type TileDbEventStore struct {
	context *tiledb.Context
	uri     string
}

func NewTiledbEventStore(event uuid.UUID) (CcEventStore, error) {
	config, err := tiledb.NewConfig()
	if err != nil {
		return nil, err
	}

	awsconfig := buildS3Config(CcProfile)
	if awscreds, ok := awsconfig.Credentials.(filesapi.S3FS_Static); ok {
		config.Set("vfs.s3.region", awsconfig.S3Region)
		config.Set("vfs.s3.aws_access_key_id", awscreds.S3Id)
		config.Set("vfs.s3.aws_secret_access_key", awscreds.S3Key)
	} else {
		return nil, errors.New("Tiledb Event Store only supports Static Credentials")
	}

	rootPath := os.Getenv(CcRootPath)
	if rootPath == "" {
		rootPath = remoteRootPath //set to default
	}

	uri := fmt.Sprintf("%s/%s/eventdb", rootPath, event.String())

	context, err := tiledb.NewContext(config)
	if err != nil {
		return nil, err
	}

	return &TileDbEventStore{context, uri}, nil
}

func (tdb *TileDbEventStore) WriteArray(input WriteArrayInput) error {
	domain, err := tiledb.NewDomain(tdb.context)
	if err != nil {
		return err
	}
	tiledbtype := GolangType2TileDbType(input.Buffer)
	arraydim, err := tiledb.NewDimension(tdb.context, "array", tiledbtype, []int32{1, int32(input.Size)}, int32(1))
	if err != nil {
		return err
	}
	//rowDim, _ := tiledb.NewDimension(tdb.context, "rows", []int32{1, 4}, int32(4))
	//colDim, _ := tiledb.NewDimension(tdb.context, "cols", []int32{1, 4}, int32(4))
	err = domain.AddDimensions(arraydim)
	if err != nil {
		return err
	}

	attribute, err := tiledb.NewAttribute(tdb.context, input.AttributeName, tiledbtype)
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

	array, err := tiledb.NewArray(tdb.context, input.DataPath)
	if err != nil {
		return err
	}
	defer array.Close()

	array.Create(arraySchema)

	array.Open(tiledb.TILEDB_WRITE)

	query, err := tiledb.NewQuery(tdb.context, array)
	if err != nil {
		return err
	}

	if err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR); err != nil {
		return err
	}

	_, err = query.SetDataBuffer(input.AttributeName, input.Buffer)
	if err != nil {
		return err
	}

	err = query.Submit()
	if err != nil {
		return err
	}
	return nil
}

func (tdb *TileDbEventStore) ReadArray(input ReadArrayInput) error {
	ctx, _ := tiledb.NewContext(nil)

	// Prepare the array for reading
	array, _ := tiledb.NewArray(tdb.context, input.DataPath)
	array.Open(tiledb.TILEDB_READ)

	// Slice only rows 1, 2 and cols 2, 3, 4
	//subArray := []int32{1, 2, 2, 4}

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 4)

	// Prepare the query
	query, _ := tiledb.NewQuery(tdb.context, array)
	//query.SetSubArray(subArray)
	query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	query.SetDataBuffer(input.AttributeName, data)

	// Submit the query and close the array.
	query.Submit()
	array.Close()

	// Print out the results.
	fmt.Println(data)
	return nil
}

func GolangType2TileDbType(buf any) tiledb.Datatype {
	return tiledb.TILEDB_INT32
}

func ccStoreArrayType2Tiledbtype(ccArrayType ARRAY_TYPE) tiledb.ArrayType {
	return tiledb.TILEDB_DENSE
}
