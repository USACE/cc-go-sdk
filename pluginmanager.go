package cc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	CcManifestId       = "CC_MANIFEST_ID"
	CcEventNumber      = "CC_EVENT_NUMBER"
	CcEventID          = "CC_EVENT_ID"
	CcPluginDefinition = "CC_PLUGIN_DEFINITION"
	CcProfile          = "CC"
	CcPayloadFormatted = "CC_PAYLOAD_FORMATTED"
	AwsAccessKeyId     = "AWS_ACCESS_KEY_ID"
	AwsSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	AwsDefaultRegion   = "AWS_DEFAULT_REGION"
	AwsS3Bucket        = "AWS_S3_BUCKET"
)

// PluginManager is a Manager designed to simplify access to stores and usage of plugin api calls
type PluginManager struct {
	ws         CcStore
	manifestId string
	logger     Logger
	payload    Payload
}

func InitPluginManager() (*PluginManager, error) {
	var manager PluginManager
	//manager.stores = make(map[string]interface{})
	sender := os.Getenv(CcPluginDefinition) //@TODO what is this used for?
	manager.logger = Logger{
		ErrorFilter: INFO,
		Sender:      sender,
	}
	manager.manifestId = os.Getenv(CcManifestId) //consider removing this from the s3store - passing a reference
	s3Store, err := NewCcStore()
	if err != nil {
		manager.logger.LogError(Error{
			ErrorLevel: INFO,
			Error:      "Unable to load the primary Compute-Store",
		})
		return nil, err
	}
	manager.ws = s3Store
	manager.payload, err = s3Store.GetPayload()
	if err != nil {
		manager.logger.LogError(Error{
			ErrorLevel: INFO,
			Error:      "Warning: Unable to load a payload!",
		})
	} else {
		for i, ds := range manager.payload.Stores {
			switch ds.StoreType {
			case S3:
				s3store, err := NewS3DataStore(ds)
				if err != nil {
					manager.logger.LogError(Error{
						ErrorLevel: WARN,
						Error:      "Warning: Unable to load a payload!",
					})
					return nil, err
				}
				manager.payload.Stores[i].Session = s3store
			case WS, RDBMS:
				manager.logger.LogMessage(Message{"WS and RDBMS session intantiation is the responsibility of the plugin."})
			default:
				errMsg := fmt.Sprintf("%s is an invalid Store Type", ds.StoreType)
				manager.logger.LogError(Error{
					ErrorLevel: ERROR,
					Error:      errMsg,
				})
				return nil, errors.New(errMsg)
			}
		}
	}
	return &manager, nil
}

// GetPayload produces a Payload for the current manifestId of the environment from S3 based on the remoteRootPath set in the configuration of the environment.
func (pm PluginManager) GetPayload() Payload {
	return pm.payload
}

func (pm PluginManager) GetInputDataSource(name string) (DataSource, error) {
	return findDs(name, pm.payload.Inputs)
}

func (pm PluginManager) GetOutputDataSource(name string) (DataSource, error) {
	return findDs(name, pm.payload.Outputs)
}

func (pm PluginManager) GetInputDataSources() []DataSource {
	return pm.payload.Inputs
}

func (pm PluginManager) GetOutputDataSources() []DataSource {
	return pm.payload.Outputs
}

func (pm PluginManager) GetFileStore(name string) (FileDataStore, error) {
	return getSession[FileDataStore](&pm, name)
}

func (pm PluginManager) GetStore(name string) (*DataStore, error) {
	return getStore(&pm, name)
}

func (pm PluginManager) GetFile(ds DataSource, path int) ([]byte, error) {
	store, err := getSession[FileDataStore](&pm, ds.StoreName)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	reader, err := store.Get(ds.Paths[path])
	_, err = buf.ReadFrom(reader)
	return buf.Bytes(), err
}

func (pm PluginManager) PutFile(data []byte, ds DataSource, path int) error {
	store, err := getSession[FileDataStore](&pm, ds.StoreName)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(data)
	return store.Put(reader, ds.Paths[path])
}

func (pm PluginManager) FileWriter(srcReader io.Reader, destDs DataSource, destPath int) error {
	store, err := getSession[FileDataStore](&pm, destDs.StoreName)
	if err != nil {
		return err
	}
	return store.Put(srcReader, destDs.Paths[0])
}

func (pm PluginManager) FileReader(ds DataSource, path int) (io.ReadCloser, error) {
	store, err := getSession[FileDataStore](&pm, ds.StoreName)
	if err != nil {
		return nil, err
	}

	reader, err := store.Get(ds.Paths[path])

	return reader, err
}

func (pm PluginManager) FileReaderByName(dataSourceName string, path int) (io.ReadCloser, error) {
	ds, err := findDs(dataSourceName, pm.payload.Inputs)
	if err != nil {
		return nil, err
	}

	store, err := getSession[FileDataStore](&pm, ds.StoreName)
	if err != nil {
		return nil, err
	}

	reader, err := store.Get(ds.Paths[path])

	return reader, err
}

func (pm PluginManager) EventNumber() int {
	if event, ok := pm.payload.Attributes[CcEventNumber]; ok {
		return event.(int)
	}
	return -1
}

func (pm PluginManager) ReportProgress(status StatusReport) {
	pm.logger.ReportProgress(status)
}
func (pm PluginManager) LogMessage(message Message) {
	pm.logger.LogMessage(message)
}
func (pm PluginManager) LogError(err Error) {
	pm.logger.LogError(err)
}

func getStore(pm *PluginManager, name string) (*DataStore, error) {
	for _, s := range pm.payload.Stores {
		if s.Name == name {
			return &s, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Store %s does not exist.\n", name))
}

func getSession[T any](pm *PluginManager, name string) (T, error) {
	for _, s := range pm.payload.Stores {
		if s.Name == name {
			return s.Session.(T), nil
		}
	}
	var store T
	return store, errors.New(fmt.Sprintf("Session %s does not exist.\n", name))
}

func findDs(name string, sources []DataSource) (DataSource, error) {
	for _, ds := range sources {
		if ds.Name == name {
			return ds, nil
		}
	}
	return DataSource{}, errors.New(fmt.Sprintf("Invalid DataSource Name: %s\n", name))
}
