package wat

import "errors"

// PluginManager is a facade designed to simplify access and usage of plugin api calls
type PluginManager struct {
	stores []WatStore
}

func InitPluginManager() (PluginManager, error) {
	var manager PluginManager
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

// PushLocalObject takes a file by name from the localRootPath (see RootPath) and pushes it into S3 to the remoteRootPath concatenated with the manifestId
func (pm PluginManager) PushObject(datasource DataSource) error {
	for _, ws := range pm.stores {
		if ws.HandlesDataSource(datasource) {
			return ws.PushObject(datasource.Name)
		}
	}
	return errors.New("no store handles this datasource")
}

// PushObjectBytes takes a slice of bytes as the input data, and a DataSource and pushes the object to the remotePathRoot concatenated with the manifestId and the DataSource Name
func (pm PluginManager) PushObjectBytes(data []byte, datasource DataSource) error {
	for _, ws := range pm.stores {
		if ws.HandlesDataSource(datasource) {
			return ws.PushObjectBytes(data, datasource)
		}
	}
	return errors.New("no store handles this datasource")
}

// GetObject takes a file name as input and builds a key based on the remoteRootPath, the manifestid and the file name to find an object on S3 and returns the bytes of that object.
func (pm PluginManager) GetObject(datasource DataSource) ([]byte, error) {
	for _, ws := range pm.stores {
		if ws.HandlesDataSource(datasource) {
			return ws.GetObject(datasource.Name)
		}
	}
	bytes := make([]byte, 0)
	return bytes, errors.New("no store handles this datasource")
}

// GetPayload produces a Payload for the current manifestId of the environment from S3 based on the remoteRootPath set in the configuration of the environment.
func (pm PluginManager) GetPayload() (Payload, error) {
	ds := DataSource{
		StoreType: s3StoreType,
	}

	for _, ws := range pm.stores {
		if ws.HandlesDataSource(ds) {
			return ws.GetPayload()
		}
	}
	var payload Payload
	return payload, errors.New("no s3Store in stores")
}

// PullObject takes a filename input, searches for that file on S3 and copies it to the local directory if a file of that name is found in the remote store.
func (pm PluginManager) PullObject(datasource DataSource) error {
	for _, ws := range pm.stores {
		if ws.HandlesDataSource(datasource) {
			return ws.PullObject(datasource.Name)
		}
	}
	return errors.New("no store handles this datasource")
}
