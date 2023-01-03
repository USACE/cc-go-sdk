package wat

import (
	"errors"
	"os"
	"strconv"
)

const (
	WatManifestId       = "WAT_MANIFEST_ID"
	WatEventNumber      = "WAT_EVENT_NUMBER"
	WatEventID          = "WAT_EVENT_ID"
	WatPluginDefinition = "WAT_PLUGIN_DEFINITION"
)

// PluginManager is a Manager designed to simplify access to stores and usage of plugin api calls
type PluginManager struct {
	stores      []WatStore
	eventNumber int
	manifestId  string
	logger      Logger
}

func InitPluginManager() (PluginManager, error) {
	var manager PluginManager
	sender := os.Getenv(WatPluginDefinition)
	manager.logger = Logger{
		ErrorFilter: INFO,
		Sender:      sender,
	}
	//get env variables
	manager.manifestId = os.Getenv(WatManifestId) //consider removing this from the s3watstore - passing a reference
	en, err := strconv.Atoi(os.Getenv(WatEventNumber))
	if err != nil {
		manager.logger.LogError(Error{
			ErrorLevel: INFO,
			Error:      "no event number was found in the environment variables",
		})
	}
	manager.eventNumber = en
	manager.stores = make([]WatStore, 0)
	s3Store, err := NewS3WatStore()
	hasOneStore := false
	if err == nil {
		hasOneStore = true
		manager.stores = append(manager.stores, s3Store)
	}
	//make other watstores and add them to the manager.

	if hasOneStore {
		return manager, nil
	}
	return manager, errors.New("no stores were added from the environment configurations")
}
func (pm PluginManager) EventNumber() int {
	return pm.eventNumber
}

// PutObject takes a datasource and data and pushes it into S3 based on the instructions in the datasource
func (pm PluginManager) PutObject(datasource DataSource, data []byte) error {
	for _, ws := range pm.stores {
		if ws.HandlesDataStoreType(datasource.StoreType) {
			poi := PutObjectInput{
				FileName:             datasource.Name,
				FileExtension:        "unknown", //how do i reconcile multiple paths in a datasource?
				DestinationStoreType: datasource.StoreType,
				ObjectState:          Memory,
				Data:                 data,
				SourcePath:           datasource.Paths[0], //how do i know if it is a local path or not
				DestPath:             datasource.Paths[0],
			}
			return ws.PutObject(poi)
		}
	}
	return errors.New("no store handles this datasource")
}

// PutObject takes a datasource and data and pushes it into S3 based on the instructions in the datasource
func (pm PluginManager) PutLocalObject(datasource DataSource) error {
	for _, ws := range pm.stores {
		if ws.HandlesDataStoreType(datasource.StoreType) {
			poi := PutObjectInput{
				FileName:             datasource.Name,
				FileExtension:        "unknown", //how do i reconcile multiple paths in a datasource?
				DestinationStoreType: datasource.StoreType,
				ObjectState:          LocalDisk,
				SourcePath:           datasource.Paths[0], //how do i know if it is a local path or not
				DestPath:             datasource.Paths[0],
			}
			return ws.PutObject(poi)
		}
	}
	return errors.New("no store handles this datasource")
}

// GetObject takes a file name as input and builds a key based on the remoteRootPath, the manifestid and the file name to find an object on S3 and returns the bytes of that object.
func (pm PluginManager) GetObject(datasource DataSource) ([]byte, error) {
	for _, ws := range pm.stores {
		if ws.HandlesDataStoreType(datasource.StoreType) {
			goi := GetObjectInput{
				SourceStoreType: datasource.StoreType,
				SourceRootPath:  datasource.EnvPrefix, //what is an env prefix really
				FileName:        datasource.Name,      //what is a name
				FileExtension:   datasource.Paths[0],  //@TODO how are we really handling multiple paths
			}
			return ws.GetObject(goi)
		}
	}
	bytes := make([]byte, 0)
	return bytes, errors.New("no store handles this datasource")
}

// GetPayload produces a Payload for the current manifestId of the environment from S3 based on the remoteRootPath set in the configuration of the environment.
func (pm PluginManager) GetPayload() (Payload, error) {
	for _, ws := range pm.stores {
		if ws.HandlesDataStoreType(S3) {
			return ws.GetPayload()
		}
	}
	var payload Payload
	return payload, errors.New("no s3Store in stores")
}

// PullObject takes a filename input, searches for that file on S3 and copies it to the local directory if a file of that name is found in the remote store.
func (pm PluginManager) PullObject(datasource DataSource) error {
	for _, ws := range pm.stores {
		if ws.HandlesDataStoreType(datasource.StoreType) {
			poi := PullObjectInput{
				SourceStoreType:     datasource.StoreType,
				SourceRootPath:      datasource.EnvPrefix, //what is an env prefix really
				FileName:            datasource.Name,      //what is a name
				FileExtension:       datasource.Paths[0],  //@TODO how are we really handling multiple paths
				DestinationRootPath: "/data",              //i think this should be a configured env variable.
			}
			return ws.PullObject(poi)
		}
	}
	return errors.New("no store handles this datasource")
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
