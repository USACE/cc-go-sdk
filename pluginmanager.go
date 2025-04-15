package cc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

const (
	CcPayloadId         = "CC_PAYLOAD_ID"
	CcManifestId        = "CC_MANIFEST_ID"
	CcEventIdentifier   = "CC_EVENT_IDENTIFIER"
	CcEventNumber       = "CC_EVENT_NUMBER"
	CcPluginDefinition  = "CC_PLUGIN_DEFINITION"
	CcProfile           = "CC"
	CcPayloadFormatted  = "CC_PAYLOAD_FORMATTED"
	CcRootPath          = "CC_ROOT"
	CcLogIdentifier     = "CC_LOG"
	AwsAccessKeyId      = "AWS_ACCESS_KEY_ID"
	AwsSecretAccessKey  = "AWS_SECRET_ACCESS_KEY"
	AwsDefaultRegion    = "AWS_DEFAULT_REGION"
	AwsS3Bucket         = "AWS_S3_BUCKET"
	AwsS3Mock           = "S3_MOCK"
	AwsS3ForcePathStyle = "S3_FORCE_PATH_STYLE"
	AwsS3DisableSSL     = "S3_DISABLE_SSL"
	AwsS3Endpoint       = "AWS_ENDPOINT"
)

var substitutionRegex string = `{([^{}]*)}`
var rx *regexp.Regexp
var maxretry int = 100

// PluginManager is a Manager designed to simplify access to stores and usage of plugin api calls
type PluginManager struct {
	EventIdentifier string
	ccStore         CcStore
	Logger          *CcLogger
	Payload
}

type PluginManagerConfig struct {
	MaxRetry int
}

func InitPluginManagerWithConfig(config PluginManagerConfig) (*PluginManager, error) {
	maxretry = config.MaxRetry
	return InitPluginManager()
}

func connectStores(stores *[]DataStore) error {
	for i, ds := range *stores {
		newInstance, err := DataStoreTypeRegistry.New(ds.StoreType)
		if err != nil {
			return err
		}
		if cds, ok := newInstance.(ConnectionDataStore); ok {
			conn, err := cds.Connect(ds)
			if err != nil {
				return err
			}
			(*stores)[i].Session = conn
		}
	}
	return nil
}

func InitPluginManager() (*PluginManager, error) {
	manifestId := os.Getenv(CcManifestId)
	payloadId := os.Getenv(CcPayloadId)
	registerStoreTypes()
	rx, _ = regexp.Compile(substitutionRegex)
	var manager PluginManager
	manager.EventIdentifier = os.Getenv(CcEventIdentifier)
	manager.Logger = NewCcLogger(CcLoggerInput{manifestId, payloadId, nil})
	s3Store, err := NewCcStore()
	if err != nil {
		return nil, err
	}
	manager.ccStore = s3Store
	payload, err := s3Store.GetPayload()
	if err != nil {
		return nil, err
	}

	manager.IOManager = payload.IOManager //@TODO do I absolutely need these two lines?
	manager.Actions = payload.Actions

	//make connections to the plugin manager stores
	err = connectStores(&manager.Stores)
	if err != nil {
		return nil, err
	}

	for i := range manager.Actions {
		//add the pm manager IOManager as a parent to the action IOManager
		//so that the action IOManager can recursively search through parent
		//IOManager elements
		manager.Actions[i].IOManager.SetParent(&manager.IOManager)

		//make connection to the action stores
		err = connectStores(&manager.Actions[i].Stores)
		if err != nil {
			return nil, err
		}
	}

	err = manager.substitutePathVariables()
	return &manager, err
}

// -----------------------------------------------
// Wrapped IOManager functions
// -----------------------------------------------

func (pm PluginManager) GetStore(name string) (*DataStore, error) {
	return pm.IOManager.GetStore(name)
}

func (pm PluginManager) GetDataSource(input GetDsInput) (DataSource, error) {
	return pm.IOManager.GetDataSource(input)
}

func (pm PluginManager) GetInputDataSource(name string) (DataSource, error) {
	return pm.IOManager.GetInputDataSource(name)
}

func (pm PluginManager) GetOutputDataSource(name string) (DataSource, error) {
	return pm.IOManager.GetOutputDataSource(name)
}

func (pm PluginManager) GetReader(input DataSourceOpInput) (io.ReadCloser, error) {
	return pm.IOManager.GetReader(input)
}

func (pm PluginManager) Get(input DataSourceOpInput) ([]byte, error) {
	return pm.IOManager.Get(input)
}

func (pm PluginManager) Put(input PutOpInput) (int, error) {
	return pm.IOManager.Put(input)
}

func (pm PluginManager) Copy(src DataSourceOpInput, dest DataSourceOpInput) error {
	return pm.IOManager.Copy(src, dest)
}

func (pm PluginManager) CopyFileToLocal(dsName string, pathkey string, dataPathKey string, localPath string) error {
	return pm.IOManager.CopyFileToLocal(dsName, pathkey, dataPathKey, localPath)
}

func (pm PluginManager) CopyFileToRemote(input CopyFileToRemoteInput) error {
	return pm.IOManager.CopyFileToRemote(input)
}

// -----------------------------------------------
// Private utility functions
// -----------------------------------------------
func (pm *PluginManager) substitutePathVariables() error {
	for i, ds := range pm.Inputs {
		err := pathsSubstitute(&ds, pm.Attributes)
		if err != nil {
			return err
		}
		pm.Inputs[i] = ds
	}
	for i, ds := range pm.Outputs {
		err := pathsSubstitute(&ds, pm.Attributes)
		if err != nil {
			return err
		}
		pm.Outputs[i] = ds
	}

	for _, action := range pm.Actions {
		pm.substituteMapVariables(action.Attributes)

		for i, ds := range action.Inputs {
			err := pathsSubstitute(&ds, pm.Attributes)
			if err != nil {
				return err
			}
			action.Inputs[i] = ds
		}

		for i, ds := range action.Outputs {
			err := pathsSubstitute(&ds, pm.Attributes)
			if err != nil {
				return err
			}
			action.Outputs[i] = ds
		}
	}

	return nil
}

func (pm *PluginManager) substituteMapVariables(params map[string]any) {
	for param, val := range params {
		switch val.(type) {
		case string:
			newval, err := parameterSubstitute(val, pm.Attributes)
			if err == nil {
				params[param] = newval
			}
		case map[string]any:
			pm.substituteMapVariables(val.(map[string]any))
		}
	}
}

// @TODO add substitution for datapaths
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

	for i, p := range ds.DataPaths {
		path, err := parameterSubstitute(p, payloadAttr)
		if err != nil {
			return err
		}
		ds.DataPaths[i] = path
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
