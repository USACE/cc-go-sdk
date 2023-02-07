package cc

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/usace/filestore"
)

const (
	S3ROOT = "root"
)

type S3DataStore struct {
	fs   filestore.FileStore
	root string
}

func (s3ds *S3DataStore) Copy(destStore FileDataStore, srcpath string, destpath string) error {
	reader, err := s3ds.fs.GetObject(filestore.PathConfig{Path: srcpath})
	if err != nil {
		return err
	}
	return destStore.Put(reader, destpath)
}

func (s3ds *S3DataStore) Get(path string) (io.ReadCloser, error) {
	return s3ds.fs.GetObject(filestore.PathConfig{Path: s3ds.root + "/" + path})
}

func (s3ds *S3DataStore) Put(reader io.Reader, path string) error {
	return s3ds.fs.Upload(reader, s3ds.root+"/"+path)
}

func (s3ds *S3DataStore) Delete(path string) error {
	return s3ds.Delete(s3ds.root + "/" + path)
}

func (s3ds *S3DataStore) Session() filestore.FileStore {
	return s3ds.fs
}

func NewS3DataStore(ds DataStore) (FileDataStore, error) {
	config := filestore.S3FSConfig{
		S3Id:     os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsAccessKeyId)),
		S3Key:    os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsSecretAccessKey)),
		S3Region: os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsDefaultRegion)),
		S3Bucket: os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsS3Bucket)),
	}

	fs, err := filestore.NewFileStore(config)
	if err != nil {
		return nil, err
	}

	if root, ok := ds.Parameters[S3ROOT]; ok {
		return &S3DataStore{fs, root}, nil
	} else {
		return nil, errors.New("Missing S3 Root parameter.  Cannot create the store.")
	}
}
