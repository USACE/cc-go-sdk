package wat

import (
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	WatManifestId       = "WAT_MANIFEST_ID"
	WatEventNumber      = "WAT_EVENT_NUMBER"
	WatEventID          = "WAT_EVENT_ID"
	WatPluginDefinition = "WAT_PLUGIN_DEFINITION"
)

// PluginManager is a Manager designed to simplify access to stores and usage of plugin api calls
type PluginManager struct {
	ws         WatStore
	manifestId string
	logger     Logger
	payload    Payload
	stores     map[string]interface{}
}

func InitPluginManager() (*PluginManager, error) {
	var manager PluginManager
	manager.stores = make(map[string]interface{})
	sender := os.Getenv(WatPluginDefinition)
	manager.logger = Logger{
		ErrorFilter: INFO,
		Sender:      sender,
	}
	manager.manifestId = os.Getenv(WatManifestId) //consider removing this from the s3watstore - passing a reference
	s3Store, err := NewWatStore()
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
		for _, ds := range manager.payload.Stores {
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
				manager.stores[ds.Name] = s3store
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

//@TODO add Shutdown method!!!

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
	return GetStore[FileDataStore](&pm, name)
}
func (pm PluginManager) GetStore(name string) (interface{}, error) {
	if store, ok := pm.stores[name]; ok {
		return store, nil
	}
	return nil, errors.New(fmt.Sprintf("Store %s does not exist.\n", name))
}

func (pm PluginManager) FileWriter(srcReader io.Reader, destDs DataSource, destPath int) error {
	store, err := GetStore[FileDataStore](&pm, destDs.StoreName)
	if err != nil {
		return err
	}
	return store.Put(srcReader, destDs.Paths[0])
}

func (pm PluginManager) FileReader(ds DataSource, path int) (io.ReadCloser, error) {
	store, err := GetStore[FileDataStore](&pm, ds.StoreName)
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

	store, err := GetStore[FileDataStore](&pm, ds.StoreName)
	if err != nil {
		return nil, err
	}

	reader, err := store.Get(ds.Paths[path])

	return reader, err
}

func (pm PluginManager) EventNumber() int {
	if event, ok := pm.payload.Attributes[WatEventNumber]; ok {
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

func GetStore[T any](pm *PluginManager, name string) (T, error) {
	var store T
	if s, ok := pm.stores[name]; ok {
		return s.(T), nil
	}
	return store, errors.New(fmt.Sprintf("Unable to get store %s", name))
}

func findDs(name string, sources []DataSource) (DataSource, error) {
	for _, ds := range sources {
		if ds.Name == name {
			return ds, nil
		}
	}
	return DataSource{}, errors.New(fmt.Sprintf("Invalid DataSource Name: %s\n", name))
}
