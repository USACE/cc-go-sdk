package cc

import (
	"fmt"
	"io"
	"reflect"

	"github.com/google/uuid"
)

type DataStoreTypeRegistryMap map[StoreType]reflect.Type

func (registry DataStoreTypeRegistryMap) Register(storeType StoreType, storeInstance any) {
	registry[storeType] = reflect.TypeOf(storeInstance)
}

func (registry DataStoreTypeRegistryMap) New(storeType StoreType) (any, error) {
	if storetype, ok := registry[storeType]; ok {
		s3Value := reflect.New(storetype)
		return s3Value.Elem().Addr().Interface(), nil
	}
	return nil, fmt.Errorf("Unregistered store type: %s\n", storeType)
}

var DataStoreTypeRegistry = make(DataStoreTypeRegistryMap)

func registerStoreTypes() {
	DataStoreTypeRegistry.Register(S3, S3DataStore{})
}

type DataStore struct {
	Name       string            `json:"name,omitempty" yaml:"name"`
	ID         *uuid.UUID        `json:"id,omitempty" yaml:id omitempty`
	StoreType  StoreType         `json:"store_type,omitempty" yaml:"store_type"`
	DsProfile  string            `json:"profile,omitempty" yaml:"profile"`
	Parameters PayloadAttributes `json:"params,omitempty" yaml:"params"`
	Session    any               `json:"-" yaml:"-"` //reference to the actual connection native to the data store
}

type ConnectionDataStore interface {
	Connect(ds DataStore) (any, error)
	GetSession() any
}

type StoreReader interface {
	Get(path string, datapath string) (io.ReadCloser, error)
}

type StoreWriter interface {
	Put(srcReader io.Reader, destPath string, destDataPath string) (int, error)
}

// Reference to a specific resource in a DataStore FILE, DB, etc
// The credential attribute is the credential prefix
// used to identify credentials in the environment.
// For example "MODEL_LIBRARY" would match "MODEL_LIBRARY_AWS_ACCESS_KEY_ID"
// or an empty string to ignore a prefix match.
type DataSource struct {
	Name      string            `json:"name,omitempty" yaml:"name"`
	ID        *uuid.UUID        `json:"id,omitempty" yaml:id omitempty`
	Paths     map[string]string `json:"paths,omitempty" yaml:"paths"`
	DataPaths map[string]string `json:"data_paths,omitempty" yaml:"data_paths"`
	StoreName string            `json:"store_name,omitempty" yaml:"store_name"`
}
