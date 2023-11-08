package cc

import (
	"errors"
	"fmt"
	"io"
	"os"

	filestore "github.com/usace/filesapi"
)

const (
	S3ROOT = "root"
)

type S3DataStore struct {
	fs   filestore.FileStore
	root string
}

func (s3ds *S3DataStore) Copy(destStore FileDataStore, srcpath string, destpath string) error {
	reader, err := s3ds.fs.GetObject(filestore.GetObjectInput{
		Path: filestore.PathConfig{Path: srcpath},
	})
	if err != nil {
		return err
	}
	return destStore.Put(reader, destpath)
}

func (s3ds *S3DataStore) Get(path string) (io.ReadCloser, error) {
	return s3ds.fs.GetObject(filestore.GetObjectInput{
		Path: filestore.PathConfig{Path: s3ds.root + "/" + path},
	})
}

func (s3ds *S3DataStore) Put(reader io.ReadCloser, path string) error {
	poi := filestore.PutObjectInput{
		Source: filestore.ObjectSource{
			Reader: reader,
		},
		Dest: filestore.PathConfig{Path: s3ds.root + "/" + path},
	}
	_, err := s3ds.fs.PutObject(poi)
	return err
}

func (s3ds *S3DataStore) Delete(path string) error {
	return s3ds.Delete(s3ds.root + "/" + path)
}

func (s3ds *S3DataStore) Session() filestore.FileStore {
	return s3ds.fs
}

func NewS3DataStore(ds DataStore) (FileDataStore, error) {

	config := filestore.S3FSConfig{
		Credentials: filestore.S3FS_Static{
			S3Id:  os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsAccessKeyId)),
			S3Key: os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsSecretAccessKey)),
		},
		S3Region: os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsDefaultRegion)),
		S3Bucket: os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsS3Bucket)),
	}
	/*
		mock, err := strconv.ParseBool(os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsS3Mock)))
		if err != nil {
			return nil, err
		}
		if mock {
			disablessl, err := strconv.ParseBool(os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsS3DisableSSL)))
			if err != nil {
				return nil, err
			}
			forcepathstyle, err := strconv.ParseBool(os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsS3ForcePathStyle)))
			if err != nil {
				return nil, err
			}
			endpoint := os.Getenv(fmt.Sprintf("%s_%s", ds.DsProfile, AwsS3Endpoint))
			config.Mock = mock
			config.S3ForcePathStyle = forcepathstyle
			config.S3Endpoint = endpoint
			config.S3DisableSSL = disablessl
		}
	*/
	fs, err := filestore.NewFileStore(config)
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
