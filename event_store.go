package cc

type ARRAY_TYPE int

const (
	ARRAY_DENSE  ARRAY_TYPE = 0
	ARRAY_SPARSE ARRAY_TYPE = 1
)

type CcEventStore interface {
	WriteArray(input WriteArrayInput) error
	ReadArray(input ReadArrayInput) error
	//WriteMatrix()
	//ReadMatrix()
	//WriteScalar()
	//ReadScalar()
}

//types support
//float32, float64, int64, string, []byte,

type WriteArrayInput struct {
	Buffer        any
	Size          int64
	DataPath      string
	ArrayType     ARRAY_TYPE
	AttributeName string
}

type ReadArrayInput struct {
	Buffer        any
	DataPath      string
	AttributeName string
}
