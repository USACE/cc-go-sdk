package cc

import (
	"io"
	"reflect"

	"github.com/google/uuid"
)

type DataStoreTypeRegistryMap map[StoreType]reflect.Type

func (registry DataStoreTypeRegistryMap) Register(storeType StoreType, storeInstance any) {
	registry[storeType] = reflect.TypeOf(storeInstance)
}

func (registry DataStoreTypeRegistryMap) New(storeType StoreType) any {
	s3Value := reflect.New(registry[storeType])
	return s3Value.Elem().Addr().Interface()
}

var DataStoreTypeRegistry = make(DataStoreTypeRegistryMap)

func registerStoreTypes() {
	DataStoreTypeRegistry.Register(S3, S3DataStore{})
}

type DataStore struct {
	Name       string            `json:"name" yaml:"name"`
	ID         *uuid.UUID        `json:"id,omitempty" yaml:id omitempty`
	StoreType  StoreType         `json:"store_type" yaml:"store_type"`
	DsProfile  string            `json:"profile,omitempty" yaml:"profile"`
	Parameters PayloadAttributes `json:"params,omitempty" yaml:"params"`
	Session    any               `json:"-" yaml:"-"` //reference to the actual connection native to the data store
}

type ConnectionDataStore interface {
	Connect(ds DataStore) (any, error)
	RawSession() any
}
type FileDataStore interface {
	Copy(destStore FileDataStore, srcpath string, destpath string) error
	Get(path string) (io.ReadCloser, error)
	Put(reader io.Reader, path string) error
	Delete(path string) error
}

// Reference to a specific resourc in a DataStore FILE, DB, etc
// The credential attribute is the credential prefix
// used to identify credentials in the environment.
// For example "MODEL_LIBRARY" would match "MODEL_LIBRARY_AWS_ACCESS_KEY_ID"
// or an empty string to ignore a prefix match.
type DataSource struct {
	Name      string            `json:"name" yaml:"name"`
	ID        *uuid.UUID        `json:"id,omitempty" yaml:id omitempty`
	Paths     map[string]string `json:"paths" yaml:"paths"`
	DataPaths map[string]string `json:"data_paths" yaml:"data_paths"`
	StoreName string            `json:"store_name" yaml:"store_name"`
}
