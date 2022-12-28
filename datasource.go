package wat

import (
	"github.com/google/uuid"
)

// Generalized data sources including FILE, DB, etc
// The credential attribute is the credential prefix
// used to identify credentials in the environment.
// For example "MODEL_LIBRARY" would match "MODEL_LIBRARY_AWS_ACCESS_KEY_ID"
// or an empty string to ignore a prefix match.
type DataSource struct {
	Name string     `json:"name" yaml:"name"`
	ID   *uuid.UUID `json:"id,omitempty" yaml:id omitempty` //optional.  used primarily for topological sort based on input/output dependencies
	//DataType   string            `json:"dataType" yaml:"dataType"`   //file,db,
	StoreType  StoreType         `json:"storeType" yaml:"storeType"` //S3
	EnvPrefix  string            `json:"envPrefix,omitempty" yaml:"envPrefix"`
	Paths      []string          `json:"paths" yaml:"paths"`             //testing to support options like shapefiles which a single source consists of multiple files
	Parameters map[string]string `json:"params,omitempty" yaml:"params"` //testing this approach to work with internal path types
}
