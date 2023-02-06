package wat

// Generalized data sources including FILE, DB, etc
// The credential attribute is the credential prefix
// used to identify credentials in the environment.
// For example "MODEL_LIBRARY" would match "MODEL_LIBRARY_AWS_ACCESS_KEY_ID"
// or an empty string to ignore a prefix match.
/*
type DataSource struct {
	Name       string            `json:"name" yaml:"name"`
	ID         *uuid.UUID        `json:"id,omitempty" yaml:id omitempty` //optional.  used primarily for topological sort based on input/output dependencies
	StoreType  StoreType         `json:"storeType" yaml:"storeType"`     //S3
	DsProfile  string            `json:"dsProfile,omitempty" yaml:"dsProfile"`
	Paths      []string          `json:"paths" yaml:"paths"`             //testing to support options like shapefiles which a single source consists of multiple files
	Parameters map[string]string `json:"params,omitempty" yaml:"params"` //testing this approach to work with internal path types
}
*/
/*
type FileDataSource interface {
	Copy(dest FileDataSource) error
	Delete() error
	FileStore() filestore.FileStore
	DataSource() DataSource
}

//----------------------------------------
type DataStore2 struct {
	Name        string
	ID          *uuid.UUID
	StoreType   StoreType
	DsProfile   string
	Parameters  map[string]string
	DataSources []DataSource
}

type DataSource2 struct {
	Name  string
	Paths []string
}

//----------------------------------------
type DataStore struct {
	Name        string
	ID          *uuid.UUID
	StoreType   StoreType
	DsProfile   string
	Parameters  map[string]string
	DataSources []DataSource
}

type DataSource struct {
	Name  string
	Paths []string
}

type FileDataStore interface {
	Copy(destStore FileDataStore, srcpath string, destpath string) error
	Get(path string) (io.ReadCloser, error)
	Put(reader io.Reader, path string) error
	Delete(path string) error
}

type S3DataStore struct {
	fs filestore.FileStore
}

func (s3ds *S3DataStore) Copy(destStore FileDataStore, srcpath string, destpath string) error {
	reader, err := s3ds.fs.GetObject(filestore.PathConfig{Path: srcpath})
	if err != nil {
		return err
	}
	return destStore.Put(reader, destpath)
}

func (s3ds *S3DataStore) Get(path string) (io.ReadCloser, error) {
	return s3ds.fs.GetObject(filestore.PathConfig{Path: path})
}

func (s3ds *S3DataStore) Put(reader io.Reader, path string) error {
	return s3ds.fs.Upload(reader, path)
}

func (s3ds *S3DataStore) Delete(path string) error {
	return s3ds.Delete(path)
}

func NewS3DataStore(ds DataStore) (FileDataStore, error) {
	config := filestore.S3FSConfig{
		S3Id:     os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, awsAccessKeyId)),
		S3Key:    os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, awsSecretAccessKey)),
		S3Region: os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, awsDefaultRegion)),
		S3Bucket: os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, awsS3Bucket)),
	}

	fs, err := filestore.NewFileStore(config)
	if err != nil {
		return nil, err
	}
	return &S3DataStore{fs}, nil
}
*/
