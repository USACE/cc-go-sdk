package cc

type ARRAY_TYPE int
type ATTR_TYPE int
type DIMENSION_TYPE int

const (
	DIMENSION_INT    DIMENSION_TYPE = 0
	DIMENSION_STRING DIMENSION_TYPE = 1

	ARRAY_DENSE  ARRAY_TYPE = 0
	ARRAY_SPARSE ARRAY_TYPE = 1

	ATTR_INT64   ATTR_TYPE = 0
	ATTR_INT32   ATTR_TYPE = 1
	ATTR_INT16   ATTR_TYPE = 2
	ATTR_INT8    ATTR_TYPE = 3
	ATTR_UINT8   ATTR_TYPE = 4
	ATTR_FLOAT32 ATTR_TYPE = 5
	ATTR_FLOAT64 ATTR_TYPE = 6
	ATTR_STRING  ATTR_TYPE = 7
)

type CcEventStore interface {
	CreateArray(input CreateArrayInput) error
	PutArray(input WriteArrayInput) error
	GetArray(input ReadArrayInput) error
	//PutMatrix(input WriteMatrixInput) error
	PutMetadata(key string, val any) error
	GetMetadata(key string) (any, error)
	GetMetadata2(key string, dest any) error
	//WriteMatrix()
	//ReadMatrix()
	//WriteKVP()
	//ReadKVP()
}

/*
NOTES:
 - add gzip filters
*/

// MatrixTypeSupport
type CcEventStoreArrayTypes interface {
	float32 | float64 | int64 | int32 | int16 | int8 | uint8 | string
}

type CreateArrayInput struct {
	Attributes []CcStoreAttr
	Dimensions []Dimension
	ArrayPath  string
}

type CcStoreAttr struct {
	Name     string
	DataType ATTR_TYPE
}

type Dimension struct {
	Name          string
	DimensionType DIMENSION_TYPE
	Domain        []int32
	TileExtent    int32
}

type WriteArrayInput struct {
	Buffers     []WriteBuffer
	BufferRange []int32
	DataPath    string
	ArrayType   ARRAY_TYPE
}

type WriteBuffer struct {
	AttrName string
	Buffer   any
	Offsets  []uint64
}

type WriteArrayInput2 struct {
	Buffer any
	Size   int64
	//BufferRange DataRange
	DataPath  string
	ArrayType ARRAY_TYPE
}

type WriteMatrixInput struct {
	Buffer any
	//BufferRange DataRange
	DataPath  string
	ArrayType ARRAY_TYPE
}

type ReadArrayInput struct {
	Buffer any
	//DataRange DataRange
	DataPath    string
	Size        int
	BufferRange []int32
	AttrName    string
}
