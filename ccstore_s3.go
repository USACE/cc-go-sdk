package cc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	filestore "github.com/usace/filesapi"
)

const ()

// S3Store implements the Store interface for AWS S3, it also stores a local root, a remote root (prefix), and a manifestId to reduce name collisions.
type S3CcStore struct {
	fs             filestore.FileStore
	localRootPath  string
	remoteRootPath string
	manifestId     string
	payloadId      string
	storeType      StoreType
}

// NewCcStore produces a CcStore backed by an S3 bucket
// if no arguments are supplied, the manifestid will get loaded from the environment
// @TODO: make sure file operations use io and readers and stream chunks.  avoid large files in memory.
func NewS3CcStore(manifestArgs ...string) (CcStore, error) {
	var manifestId string
	var payloadId string
	if len(manifestArgs) > 1 {
		manifestId = manifestArgs[0]
		payloadId = manifestArgs[1]
	} else {
		manifestId = os.Getenv(CcManifestId)
		payloadId = os.Getenv(CcPayloadId)
	}
	awsconfig := BuildS3Config(CcProfile)
	rootPath := os.Getenv(CcRootPath)
	if rootPath == "" {
		rootPath = RemoteRootPath //set to default
	}

	fs, err := filestore.NewFileStore(awsconfig)
	if err != nil {
		return nil, err
	}
	return &S3CcStore{fs, localRootPath, rootPath, manifestId, payloadId, S3}, nil
}

// HandlesDataSource determines if a datasource is handled by this store
func (ws *S3CcStore) HandlesDataStoreType(storeType StoreType) bool {
	return ws.storeType == storeType
}

// RootPath provides access to the local root path where files are expected to live for operations like push and pull object.
func (ws *S3CcStore) RootPath() string {
	return ws.localRootPath
}

// PutObject takes a file by name from the localRootPath (see RootPath) and pushes it into S3 to the remoteRootPath concatenated with the manifestId
func (ws *S3CcStore) PutObject(poi PutObjectInput) error {
	s3path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s.%s", ws.remoteRootPath, ws.manifestId, poi.FileName, poi.FileExtension)}
	var data []byte
	if poi.ObjectState == LocalDisk {

		localpath := fmt.Sprintf("%s/%s.%s", ws.localRootPath, poi.FileName, poi.FileExtension)
		contents, err := os.ReadFile(localpath)
		if err != nil {
			return err
		}
		data = contents
	} else if poi.ObjectState == Memory {
		data = poi.Data
	} else {
		//handle remote to remote??
		return errors.New("not currently supporting remote to remote data transfers - use getobject to retrieve bytes and push as memory object via put object")
	}
	fspoi := filestore.PutObjectInput{
		Dest: s3path,
		Source: filestore.ObjectSource{
			Data: data,
		},
	}
	foo, err := ws.fs.PutObject(fspoi)
	if err != nil {
		log.Println(foo)
	}
	return err
}

// GetObject takes a file name as input and builds a key based on the remoteRootPath, the manifestid and the file name to find an object on S3 and returns the bytes of that object.
func (ws *S3CcStore) GetObject(input GetObjectInput) ([]byte, error) {
	path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s.%s", input.SourceRootPath, ws.manifestId, input.FileName, input.FileExtension)}
	fsgoi := filestore.GetObjectInput{
		Path: path,
	}
	reader, err := ws.fs.GetObject(fsgoi)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// GetPayload produces a Payload for the current manifestId of the environment from S3 based on the remoteRootPath set in the configuration of the environment.
func (ws *S3CcStore) GetPayload() (Payload, error) {
	payload := Payload{}
	path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s", ws.remoteRootPath, ws.payloadId, payloadFileName)}
	fsgoi := filestore.GetObjectInput{
		Path: path,
	}
	reader, err := ws.fs.GetObject(fsgoi)
	if err != nil {
		return payload, err
	}
	defer reader.Close()

	err = json.NewDecoder(reader).Decode(&payload)

	return payload, err
}

// SetPayload sets a payload. This is designed for cloud compute to use, please do not use this method in a plugin.
func (ws *S3CcStore) SetPayload(p Payload) error {
	s3path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s", ws.remoteRootPath, ws.payloadId, "payload")}
	_, shouldFormat := os.LookupEnv(CcPayloadFormatted)
	var data []byte
	var err error
	if shouldFormat {
		data, err = json.MarshalIndent(p, "", "  ")
	} else {
		data, err = json.Marshal(p)
	}
	if err != nil {
		return err
	}
	fspoi := filestore.PutObjectInput{
		Dest: s3path,
		Source: filestore.ObjectSource{
			Data: data,
		},
	}
	_, err = ws.fs.PutObject(fspoi)
	return err
}

// PullObject takes a filename input, searches for that file on S3 and copies it to the local directory if a file of that name is found in the remote store.
func (ws *S3CcStore) PullObject(input PullObjectInput) error {
	path := filestore.PathConfig{Path: fmt.Sprintf("%s/%s/%s.%s", input.SourceRootPath, ws.manifestId, input.FileName, input.FileExtension)}
	localPath := fmt.Sprintf("%s/%s.%s", input.DestinationRootPath, input.FileName, input.FileExtension)
	//open destination
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := bufio.NewWriter(f)

	//open source
	fsgoi := filestore.GetObjectInput{
		Path: path,
	}
	reader, err := ws.fs.GetObject(fsgoi)
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.Copy(writer, reader)
	return err
}

func BuildS3Config(profile string) filestore.S3FSConfig {
	awsconfig := filestore.S3FSConfig{
		Credentials: filestore.S3FS_Static{
			S3Id:  os.Getenv(fmt.Sprintf("%s_%s", profile, AwsAccessKeyId)),
			S3Key: os.Getenv(fmt.Sprintf("%s_%s", profile, AwsSecretAccessKey)),
		},
		S3Region:    os.Getenv(fmt.Sprintf("%s_%s", profile, AwsDefaultRegion)),
		S3Bucket:    os.Getenv(fmt.Sprintf("%s_%s", profile, AwsS3Bucket)),
		AltEndpoint: os.Getenv(fmt.Sprintf("%s_%s", profile, AwsS3Endpoint)),
		AwsOptions: []func(*config.LoadOptions) error{
			config.WithRetryer(func() aws.Retryer {
				return retry.AddWithMaxAttempts(retry.NewStandard(), 5)
			}),
		},
	}

	return awsconfig
}
