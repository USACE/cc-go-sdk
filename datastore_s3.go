package cc

import (
	"errors"
	"io"
	"log"

	filestore "github.com/usace/filesapi"
)

const (
	S3ROOT = "root"
)

type S3DataStore struct {
	fs   filestore.FileStore
	root string
}

/*
func (s3ds *S3DataStore) Copy(destStore FileDataStore, srcpath string, destpath string) error {
	fsgoi := filestore.GetObjectInput{
		Path: filestore.PathConfig{Path: srcpath},
	}
	reader, err := s3ds.fs.GetObject(fsgoi)
	if err != nil {
		return err
	}
	return destStore.Put(reader, destpath)
}
*/

func (s3ds *S3DataStore) Get(path string, datapath string) (io.ReadCloser, error) {
	log.Println("S3 Datastore does not use the datapath.  It will be ignored")
	fsgoi := filestore.GetObjectInput{
		Path: filestore.PathConfig{Path: s3ds.root + "/" + path},
	}

	return s3ds.fs.GetObject(fsgoi)
}

func (s3ds *S3DataStore) GetFilestore() *filestore.FileStore {
	return &s3ds.fs
}

func (s3ds *S3DataStore) Put(reader io.Reader, path string, destDataPath string) (int, error) {
	poi := filestore.PutObjectInput{
		Source: filestore.ObjectSource{
			Reader: reader,
		},
		Dest: filestore.PathConfig{Path: s3ds.root + "/" + path},
	}
	//@TODO fix the bytes transferred int
	_, err := s3ds.fs.PutObject(poi)

	return -1, err
}

func (s3ds *S3DataStore) Delete(path string) error {
	return s3ds.Delete(s3ds.root + "/" + path)
}

func (s3ds *S3DataStore) GetSession() any {
	if s3fs, ok := s3ds.fs.(*filestore.S3FS); ok {
		return s3fs.GetClient()
	}
	return nil
}

func (s3ds *S3DataStore) Connect(ds DataStore) (any, error) {
	awsconfig := BuildS3Config(ds.DsProfile)
	fs, err := filestore.NewFileStore(awsconfig)
	if err != nil {
		return nil, err
	}
	if root, ok := ds.Parameters[S3ROOT]; ok {
		if rootstr, ok := root.(string); ok {
			return &S3DataStore{fs, rootstr}, nil
		} else {
			return nil, errors.New("Invalid S3 Root parameter.  Parameter must be a string.")
		}
	} else {
		return nil, errors.New("Missing S3 Root parameter.  Cannot create the store.")
	}
}
