package wat

var localRootPath string = "/data"
var remoteRootPath string = "wat_store"

const (
	watAwsAccessKeyId     = "WAT_AWS_ACCESS_KEY_ID"
	watAwsSecretAccessKey = "WAT_AWS_SECRET_ACCESS_KEY"
	watAwsDefaultRegion   = "WAT_AWS_DEFAULT_REGION"
	watAwsS3Bucket        = "WAT_AWS_S3_BUCKET"
	watManifestId         = "WAT_MANIFEST_ID"

	payloadFileName = "payload"

	s3StoreType  = "S3"
	ebsStoreType = "EBS"
)

type WatStore interface {
	PushObject(key string) error
	PushObjectBytes(data []byte, datasource DataSource) error
	PullObject(key string) error
	GetObject(key string) ([]byte, error)
	GetPayload() (Payload, error)
	SetPayload(p Payload) error //@TODO migrate watcompute?
	RootPath() string
}

// @TODO jobid is really the manifest id
