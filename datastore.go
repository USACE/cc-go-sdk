package cc

import (
	"io"

	"github.com/google/uuid"
)

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
	DataPaths  []string          `json:"datapaths" yaml:"datapaths"`     //internal data set paths for data sources that contain multiple data sets
	Parameters map[string]string `json:"params,omitempty" yaml:"params"` //testing this approach to work with internal path types
}
*/

type DataStore struct {
	Name       string            `json:"name" yaml:"name"`
	ID         *uuid.UUID        `json:"id,omitempty" yaml:id omitempty`
	StoreType  StoreType         `json:"store_type" yaml:"store_type"`
	DsProfile  string            `json:"profile,omitempty" yaml:"profile"`
	Parameters PayloadAttributes `json:"params,omitempty" yaml:"params"`
	Session    any               `json:"-" yaml:"-"` //reference to the actual connection native to the data store
}

type FileDataStore interface {
	Copy(destStore FileDataStore, srcpath string, destpath string) error
	Get(path string) (io.ReadCloser, error)
	Put(reader io.Reader, path string) error
	Delete(path string) error
}

type DataSource struct {
	Name      string     `json:"name" yaml:"name"`
	ID        *uuid.UUID `json:"id,omitempty" yaml:id omitempty`
	Paths     []string   `json:"paths" yaml:"paths"`
	DataPaths []string   `json:"data_paths" yaml:"data_paths"`
	StoreName string     `json:"store_name" yaml:"store_name"`
}
