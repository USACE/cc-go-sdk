package cc

import (
	"errors"
	"fmt"
	"io"

	filestore "github.com/usace/filesapi"
)

const (
	S3ROOT = "root"
)

type FileDataStoreTypes interface {
	filestore.BlockFS | filestore.S3FS
}

type FileDataStore[T FileDataStoreTypes] struct {
	fs   filestore.FileStore
	root string
}

func (fds *FileDataStore[T]) Get(path string, datapath string) (io.ReadCloser, error) {
	fsgoi := filestore.GetObjectInput{
		Path: filestore.PathConfig{Path: fds.root + "/" + path},
	}

	return fds.fs.GetObject(fsgoi)
}

func (fds *FileDataStore[T]) GetFilestore() filestore.FileStore {
	return fds.fs
}

func (fds *FileDataStore[T]) Put(reader io.Reader, path string, destDataPath string) (int, error) {
	poi := filestore.PutObjectInput{
		Source: filestore.ObjectSource{
			Reader: reader,
		},
		Dest: filestore.PathConfig{Path: fds.root + "/" + path},
	}
	//@TODO fix the bytes transferred int
	_, err := fds.fs.PutObject(poi)

	return -1, err
}

func (fds *FileDataStore[T]) Delete(path string) error {
	return fds.Delete(fds.root + "/" + path) //@TODO...for real?  Does this even work?
}

func (fds *FileDataStore[T]) GetSession() any {
	switch v := any(fds.fs).(type) {
	case *filestore.S3FS:
		return v.GetClient()
	case *filestore.BlockFS:
		return nil //block file system does not return a client.  Direct calls are just that...direct to the os
	default:
		return nil
	}
}

func (fds *FileDataStore[T]) Connect(ds DataStore) (any, error) {
	switch ds.StoreType {
	case FSS3:
		awsconfig := BuildS3Config(ds.DsProfile)
		fs, err := filestore.NewFileStore(awsconfig)
		if err != nil {
			return nil, err
		}
		if root, ok := ds.Parameters[S3ROOT]; ok {
			if rootstr, ok := root.(string); ok {
				return &FileDataStore[T]{fs, rootstr}, nil //@TODO why am i returning my original type?
			} else {
				return nil, errors.New("invalid s3 root parameter.  parameter must be a string")
			}
		} else {
			return nil, errors.New("missing s3 root parameter.  cannot create the store")
		}
	case FSB:
		//no need to connect for a file store
		return nil, nil
	}

	//unsupported type
	return nil, fmt.Errorf("unsupported filestore connection")

}
