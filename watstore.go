package wat

const (
	localRootPath   = "/data"
	remoteRootPath  = "/wat_store"
	payloadFileName = "payload"
)

type StoreType string

const (
	S3  StoreType = "S3"
	EBS StoreType = "EBS"
)

type ObjectState int8

const (
	Memory    ObjectState = 0
	LocalDisk ObjectState = 1
	//RemoteDisk ObjectState = 2 //@TODO add remotedisk option for object state.
)

type WatStore interface {
	PutObject(input PutObjectInput) error
	PullObject(input PullObjectInput) error
	GetObject(input GetObjectInput) ([]byte, error)
	GetPayload() (Payload, error)
	SetPayload(p Payload) error //@TODO migrate watcompute?
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

// @TODO jobid is really the manifest id
