package cc

const (
	localRootPath   = "/data"
	RemoteRootPath  = "/cc_store"
	payloadFileName = "payload"
)

type StoreType string

const (
	S3    StoreType = "S3"
	WS    StoreType = "WS"
	RDBMS StoreType = "RDBMS"
	EBS   StoreType = "EBS"
	//@TODO ADD TileDB store type here
)

type ObjectState int8

const (
	Memory    ObjectState = 0
	LocalDisk ObjectState = 1
	//RemoteDisk ObjectState = 2 //@TODO add remotedisk option for object state.
)

type CcStore interface {
	PutObject(input PutObjectInput) error
	PullObject(input PullObjectInput) error
	GetObject(input GetObjectInput) ([]byte, error)
	GetPayload() (Payload, error)
	SetPayload(p Payload) error
	RootPath() string
	HandlesDataStoreType(datasourcetype StoreType) bool
}
type PutObjectInput struct {
	FileName             string
	FileExtension        string
	DestinationStoreType StoreType
	ObjectState          ObjectState
	Data                 []byte //optional - required if objectstate == Memory
	SourcePath           string //optional - required if objectstate != Memory
	DestPath             string
}
type GetObjectInput struct {
	SourceStoreType StoreType
	SourceRootPath  string
	FileName        string
	FileExtension   string
}
type PullObjectInput struct {
	SourceStoreType     StoreType
	SourceRootPath      string
	DestinationRootPath string
	FileName            string
	FileExtension       string
}

func NewCcStore(manifestArgs ...string) (CcStore, error) {
	return NewS3CcStore(manifestArgs...)
}

// @TODO jobid is really the manifest id
