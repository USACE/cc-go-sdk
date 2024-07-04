package cc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const (
	CcPayloadId   = "CC_PAYLOAD_ID"
	CcManifestId  = "CC_MANIFEST_ID"
	CcEventNumber = "CC_EVENT_NUMBER"
	//CcEventID           = "CC_EVENT_ID"
	CcPluginDefinition  = "CC_PLUGIN_DEFINITION"
	CcProfile           = "CC"
	CcPayloadFormatted  = "CC_PAYLOAD_FORMATTED"
	CcRootPath          = "CC_ROOT"
	AwsAccessKeyId      = "AWS_ACCESS_KEY_ID"
	AwsSecretAccessKey  = "AWS_SECRET_ACCESS_KEY"
	AwsDefaultRegion    = "AWS_DEFAULT_REGION"
	AwsS3Bucket         = "AWS_S3_BUCKET"
	AwsS3Mock           = "S3_MOCK"
	AwsS3ForcePathStyle = "S3_FORCE_PATH_STYLE"
	AwsS3DisableSSL     = "S3_DISABLE_SSL"
	AwsS3Endpoint       = "S3_ENDPOINT"
)

var substitutionRegex string = `{([^{}]*)}`
var rx *regexp.Regexp
var maxretry int = 100

// PluginManager is a Manager designed to simplify access to stores and usage of plugin api calls
type PluginManager struct {
	ws         CcStore
	manifestId string
	logger     Logger
	payload    Payload
}

type PluginManagerConfig struct {
	MaxRetry int
}

func InitPluginManagerWithConfig(config PluginManagerConfig) (*PluginManager, error) {
	maxretry = config.MaxRetry
	return InitPluginManager()
}

func InitPluginManager() (*PluginManager, error) {
	rx, _ = regexp.Compile(substitutionRegex)
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
			Error:      fmt.Sprintf("Warning: Unable to load the payload: %s\n", err),
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
	err = manager.substitutePathVariables()
	return &manager, err
}

func (pm PluginManager) CopyToLocal(ds DataSource, pathIndex int, localPath string) error {
	reader, err := pm.FileReader(ds, pathIndex)
	if err != nil {
		return err
	}
	defer reader.Close()

	writer, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, reader)
	return err
}

func (pm PluginManager) CopyToRemote(localpath string, ds DataSource, pathindex int) error {
	reader, err := os.Open(localpath)
	if err != nil {
		return err
	}
	return pm.FileWriter(reader, ds, pathindex)
}

// GetPayload produces a Payload for the current manifestId of the environment from S3 based on the remoteRootPath set in the configuration of the environment.
func (pm PluginManager) GetPayload() Payload {
	return pm.payload
}

func (pm PluginManager) GetInputDataSource(name string) (DataSource, error) {
	return findDs(name, pm.payload.Inputs)
	/*
		ds, err := findDs(name, pm.payload.Inputs)
		if err != nil {
			return ds, err
		}
		err = pm.pathsSubstitute(&ds)
		return ds, err
	*/
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
	if err != nil {
		return nil, err
	}
	_, err = buf.ReadFrom(reader)
	return buf.Bytes(), err
}

func (pm PluginManager) GetFileByName(dataSourceName string, path int) ([]byte, error) {
	ds, err := findDs(dataSourceName, pm.payload.Inputs)
	if err != nil {
		return nil, err
	}

	store, err := getSession[FileDataStore](&pm, ds.StoreName)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	reader, err := store.Get(ds.Paths[path])
	if err != nil {
		return nil, err
	}
	_, err = buf.ReadFrom(reader)
	return buf.Bytes(), err
}

func (pm PluginManager) PutFile(data []byte, ds DataSource, path int) error {
	store, err := getSession[FileDataStore](&pm, ds.StoreName)
	if err != nil {
		return err
	}
	//reader := io.NopCloser(bytes.NewReader(data))
	reader := bytes.NewReader(data)
	return store.Put(reader, ds.Paths[path])
}

func (pm PluginManager) FileWriter(srcReader io.Reader, destDs DataSource, destPath int) error {
	store, err := getSession[FileDataStore](&pm, destDs.StoreName)
	if err != nil {
		return err
	}
	return store.Put(srcReader, destDs.Paths[destPath])
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
	//try reading from payload attribute first
	if event, ok := pm.payload.Attributes[CcEventNumber]; ok {
		return int(event.(float64))
	}

	//fall back to envrionment variable
	sidx := os.Getenv(CcEventNumber)
	eventNumber, err := strconv.Atoi(sidx)
	if err != nil {
		eventNumber = -1
	}
	return eventNumber
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

func (pm *PluginManager) substitutePathVariables() error {
	for i, ds := range pm.payload.Inputs {
		err := pathsSubstitute(&ds, pm.payload.Attributes)
		if err != nil {
			return err
		}
		pm.payload.Inputs[i] = ds
	}
	for i, ds := range pm.payload.Outputs {
		err := pathsSubstitute(&ds, pm.payload.Attributes)
		if err != nil {
			return err
		}
		pm.payload.Outputs[i] = ds
	}

	for _, action := range pm.payload.Actions {
		pm.substituteMapVariables(action.Parameters)
	}

	return nil
}

func (pm *PluginManager) substituteMapVariables(params map[string]any) {
	for param, val := range params {
		switch val.(type) {
		case string:
			newval, err := parameterSubstitute(val, pm.payload.Attributes)
			if err == nil {
				params[param] = newval
			}
		case map[string]any:
			pm.substituteMapVariables(val.(map[string]any))
		}
	}
}

func pathsSubstitute(ds *DataSource, payloadAttr map[string]any) error {
	name, err := parameterSubstitute(ds.Name, payloadAttr)
	if err != nil {
		return err
	}
	ds.Name = name
	for i, p := range ds.Paths {
		path, err := parameterSubstitute(p, payloadAttr)
		if err != nil {
			return err
		}
		ds.Paths[i] = path
	}
	return nil
}

func parameterSubstitute(param interface{}, payloadAttr map[string]any) (string, error) {
	switch param.(type) {
	case string:
		strparam := param.(string)
		result := rx.FindAllStringSubmatch(strparam, -1)
		for _, match := range result {
			sub := strings.Split(match[1], "::")
			if len(sub) != 2 {
				return "", errors.New(fmt.Sprintf("Invalid Data Source Substitution: %s\n", match[0]))
			}
			val := ""
			switch sub[0] {
			case "ENV":
				val = os.Getenv(sub[1])
				if val == "" {
					return "", errors.New(fmt.Sprintf("Invalid Data Source Substitution.  Missing environment parameter: %s\n", match[0]))
				}
			case "ATTR":
				val2, ok := payloadAttr[sub[1]]
				if !ok {
					return "", errors.New(fmt.Sprintf("Invalid Data Source Substitution.  Missing payload parameter: %s\n", match[0]))
				}
				val = fmt.Sprintf("%v", val2) //need to coerce non-string values into strings.  for example ints might be perfectly valid for parameter substitution in a url
			default:
				return "", errors.New(fmt.Sprintf("Invalid Data Source Substitution: %s\n", match[0]))
			}

			strparam = strings.Replace(strparam, match[0], val, 1)
		}
		return strparam, nil
	default:
		return "", errors.New("Invalid parameter type")
	}
}
