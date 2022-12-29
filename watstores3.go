package wat

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/usace/filestore"
)

const (
	watAwsAccessKeyId     = "WAT_AWS_ACCESS_KEY_ID"
	watAwsSecretAccessKey = "WAT_AWS_SECRET_ACCESS_KEY"
	watAwsDefaultRegion   = "WAT_AWS_DEFAULT_REGION"
	watAwsS3Bucket        = "WAT_AWS_S3_BUCKET"
)

// S3WatStore implements the WatStore interface for AWS S3, it also stores a local root, a remote root (prefix), and a manifestId to reduce name collisions.
type S3WatStore struct {
	fs             filestore.FileStore
	localRootPath  string
	remoteRootPath string
	manifestId     string
	storeType      StoreType
}

// NewS3WatStore produces a WatStore backed by an S3 bucket based on environment variables.
// @TODO: Switch to aws golang v2 s3 api and use profile for connection?????
// @TODO: make sure file operations use io and readers and stream chunks.  avoid large files in memory.
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
	return &S3WatStore{fs, localRootPath, remoteRootPath, manifestId, S3}, nil
}

// HandlesDataSource determines if a datasource is handled by this store
func (ws *S3WatStore) HandlesDataStoreType(storeType StoreType) bool {
	return ws.storeType == storeType
}

// RootPath provides access to the local root path where files are expected to live for operations like push and pull object.
func (ws *S3WatStore) RootPath() string {
	return ws.localRootPath
}

// PutObject takes a file by name from the localRootPath (see RootPath) and pushes it into S3 to the remoteRootPath concatenated with the manifestId
func (ws *S3WatStore) PutObject(poi PutObjectInput) error {
	s3path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s.%s", ws.remoteRootPath, ws.manifestId, poi.FileName, poi.FileExtension)}
	localpath := fmt.Sprintf("%s/%s.%s", ws.localRootPath, poi.FileName, poi.FileExtension)
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

// GetObject takes a file name as input and builds a key based on the remoteRootPath, the manifestid and the file name to find an object on S3 and returns the bytes of that object.
func (ws *S3WatStore) GetObject(filename string) ([]byte, error) {
	path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s", ws.remoteRootPath, ws.manifestId, filename)}
	reader, err := ws.fs.GetObject(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

// GetPayload produces a Payload for the current manifestId of the environment from S3 based on the remoteRootPath set in the configuration of the environment.
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

// SetPayload sets a payload. This is designed for watcompute to use, please do not use this method in a plugin.
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

// PullObject takes a filename input, searches for that file on S3 and copies it to the local directory if a file of that name is found in the remote store.
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
