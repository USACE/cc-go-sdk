package wat

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/usace/filestore"
)

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

//Generalized data sources including FILE, DB, etc
//The credential attribute is the credential prefix
//used to identify credentials in the environment.
//For example "MODEL_LIBRARY" would match "MODEL_LIBRARY_AWS_ACCESS_KEY_ID"
//or an empty string to ignore a prefix match.
type DataSource struct {
	Name string     `json:"name" yaml:"name"`
	ID   *uuid.UUID `json:"id,omitempty" yaml:id omitempty` //optional.  used primarily for topological sort based on input/output dependencies
	//DataType   string            `json:"dataType" yaml:"dataType"`   //file,db,
	StoreType  string            `json:"storeType" yaml:"storeType"` //S3
	EnvPrefix  string            `json:"envPrefix,omitempty" yaml:"envPrefix"`
	Paths      []string          `json:"paths" yaml:"paths"`             //testing to support options like shapefiles which a single source consists of multiple files
	Parameters map[string]string `json:"params,omitempty" yaml:"params"` //testing this approach to work with internal path types
}

type Payload struct {
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Inputs     []DataSource           `json:"inputs"`
	Outputs    []DataSource           `json:"outputs"`
}

//@TODO jobid is really the manifest id
type WatStore interface {
	PushObject(key string) error
	PullObject(key string) error
	GetObject(key string) ([]byte, error)
	GetPayload() (Payload, error)
	SetPayload(p Payload) error //@TODO migrate watcompute?
}

type S3WatStore struct {
	fs             filestore.FileStore
	localRootPath  string
	remoteRootPath string
	manifestId     string
}

//@TODO: Switch to aws golang v2 s3 api and use profile for connection?????
//@TODO: make sure file operations use io and readers and stream chunks.  avoid large files in memory.
func NewS3WatStore() (WatStore, error) {
	manifestId := os.Getenv(watManifestId)
	config := filestore.S3FSConfig{
		S3Id:     os.Getenv(watAwsAccessKeyId),
		S3Key:    os.Getenv(watAwsSecretAccessKey),
		S3Region: os.Getenv(watAwsDefaultRegion),
		S3Bucket: os.Getenv(watAwsS3Bucket),
	}

	fs, err := filestore.NewFileStore(config)
	if err != nil {
		return nil, err
	}
	return &S3WatStore{fs, localRootPath, remoteRootPath, manifestId}, nil
}

func (ws *S3WatStore) PushObject(filename string) error {
	s3path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s", ws.remoteRootPath, ws.manifestId, filename)}
	localpath := fmt.Sprintf("%s/%s", ws.localRootPath, filename)
	data, err := os.ReadFile(localpath)
	if err != nil {
		return err
	}
	foo, err := ws.fs.PutObject(s3path, data)
	if err != nil {
		log.Println(foo)
	}
	return err
}

func (ws *S3WatStore) GetObject(filename string) ([]byte, error) {
	path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s", ws.remoteRootPath, ws.manifestId, filename)}
	reader, err := ws.fs.GetObject(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

func (ws *S3WatStore) GetPayload() (Payload, error) {
	payload := Payload{}
	path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s", ws.remoteRootPath, ws.manifestId, payloadFileName)}
	reader, err := ws.fs.GetObject(path)
	if err != nil {
		return payload, err
	}
	defer reader.Close()

	err = json.NewDecoder(reader).Decode(&payload)

	return payload, err
}

func (ws *S3WatStore) SetPayload(p Payload) error {
	s3path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s", ws.remoteRootPath, ws.manifestId, "payload")}

	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	foo, err := ws.fs.PutObject(s3path, data)
	if err != nil {
		log.Println(foo)
	}
	return err
}

func (ws *S3WatStore) PullObject(filename string) error {
	path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s", ws.remoteRootPath, ws.manifestId, filename)}
	localPath := fmt.Sprintf("%s/%s", ws.localRootPath, filename)
	//open destination
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := bufio.NewWriter(f)

	//open source
	reader, err := ws.fs.GetObject(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.Copy(writer, reader)
	return err
}
