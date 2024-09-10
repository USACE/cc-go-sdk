package cc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

type DataSourceIoType string

const (
	DataSourceInput  DataSourceIoType = "INPUT"
	DataSourceOutput DataSourceIoType = "OUTPUT"
	DataSourceAll    DataSourceIoType = "" //zero value == all
)

type Payload struct {
	IOManager
	Actions []Action `json:"actions"`
}

type Action struct {
	IOManager
	Type        string `json:"type"`
	Description string `json:"description"`
}

type IOManager struct {
	Attributes PayloadAttributes `json:"attributes,omitempty"`
	Stores     []DataStore       `json:"stores"`
	Inputs     []DataSource      `json:"inputs"`
	Outputs    []DataSource      `json:"outputs"`
}

type GetDsInput struct {
	DsIoType DataSourceIoType
	DsName   string
}

type DataSourceOpInput struct {
	DataSourceName string
	PathKey        string
	DataPathKey    string
}

type PutOpInput struct {
	SrcReader io.Reader
	DataSourceOpInput
}

func (im *IOManager) GetStore(name string) (*DataStore, error) {
	for _, store := range im.Stores {
		if store.Name == name {
			return &store, nil
		}
	}
	return nil, errors.New("Invalid store name")
}

func (im *IOManager) GetDataSource(input GetDsInput) (DataSource, error) {
	sources := []DataSource{}
	switch input.DsIoType {
	case DataSourceInput:
		sources = im.Inputs
	case DataSourceOutput:
		sources = im.Outputs
	case DataSourceAll:
		sources = append(sources, im.Inputs...)
		sources = append(sources, im.Outputs...)
	}
	for _, ds := range sources {
		if input.DsName == ds.Name {
			return ds, nil
		}
	}
	return DataSource{}, errors.New(fmt.Sprintf("Data source %s not found", input.DsName))
}

func (im *IOManager) GetInputDataSource(name string) (DataSource, error) {
	return im.GetDataSource(GetDsInput{DataSourceInput, name})
}

func (im *IOManager) GetOutputDataSource(name string) (DataSource, error) {
	return im.GetDataSource(GetDsInput{DataSourceOutput, name})
}

func (im *IOManager) GetReader(input DataSourceOpInput) (io.Reader, error) {
	dataSource, err := im.GetInputDataSource(input.DataSourceName)
	if err != nil {
		return nil, err
	}
	dataStore, err := im.GetStore(dataSource.StoreName)
	if readerStore, ok := dataStore.Session.(StoreReader); ok {
		path := dataSource.Paths[input.PathKey]
		datapath := ""
		if input.DataPathKey != "" {
			datapath = dataSource.DataPaths[input.DataPathKey]
		}
		return readerStore.Get(path, datapath)
	}
	return nil, fmt.Errorf("Data Store %s session does not implement a StoreReader", dataStore.Name)
}

func (im *IOManager) Get(input DataSourceOpInput) ([]byte, error) {
	reader, err := im.GetReader(input)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	return buf.Bytes(), nil
}

func (im *IOManager) Put(input PutOpInput) (int, error) {
	ds, err := im.GetOutputDataSource(input.DataSourceName)
	if err != nil {
		return 0, err
	}

	store, err := im.GetStore(ds.StoreName)
	if err != nil {
		return 0, err
	}

	if writer, ok := store.Session.(StoreWriter); ok {
		path := ds.Paths[input.PathKey]
		datapath := ""
		if input.DataPathKey != "" {
			datapath = ds.DataPaths[input.DataPathKey]
		}

		return writer.Put(input.SrcReader, path, datapath)
	}

	return 0, fmt.Errorf("Data Store %s session does not implement a StoreWriter", ds.StoreName)

}

func (im *IOManager) Copy(src DataSourceOpInput, dest DataSourceOpInput) error {
	srcds, err := im.GetOutputDataSource(src.DataSourceName)
	if err != nil {
		return err
	}

	srcstore, err := im.GetStore(srcds.StoreName)
	if err != nil {
		return err
	}

	destds, err := im.GetOutputDataSource(dest.DataSourceName)
	if err != nil {
		return err
	}

	deststore, err := im.GetStore(destds.StoreName)
	if err != nil {
		return err
	}

	if srcReader, ok := srcstore.Session.(StoreReader); ok {
		if destwriter, ok := deststore.Session.(StoreWriter); ok {

			//get the reader
			srcpath := srcds.Paths[src.PathKey]
			srcdatapath := ""
			if src.DataPathKey != "" {
				srcdatapath = srcds.DataPaths[src.DataPathKey]
			}
			reader, err := srcReader.Get(srcpath, srcdatapath)
			if err != nil {
				return err
			}

			//write
			destpath := destds.Paths[dest.PathKey]
			destdatapath := ""
			if dest.DataPathKey != "" {
				destdatapath = destds.DataPaths[dest.DataPathKey]
			}
			_, err = destwriter.Put(reader, destpath, destdatapath)
			return err
		}
		return fmt.Errorf("Destination Data Store %s session does not implement a StoreWriter", srcstore.Name)
	}
	return fmt.Errorf("Source Data Store %s session does not implement a StoreReader", srcstore.Name)
}

func (im *IOManager) CopyFileToLocal(dsName string, pathkey string, dataPathKey string, localPath string) error {
	ds, err := im.GetDataSource(GetDsInput{DataSourceInput, dsName})
	if err != nil {
		return err
	}

	store, err := im.GetStore(ds.StoreName)
	if err != nil {
		return err
	}

	path := ds.Paths[pathkey]
	datapath := ""
	if dataPathKey != "" {
		datapath = ds.DataPaths[dataPathKey]
	}

	if storeReader, ok := store.Session.(StoreReader); ok {
		reader, err := storeReader.Get(path, datapath)
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

	return fmt.Errorf("Data Store %s session does not implement a StoreReader", store.Name)
}

type CopyFileToRemoteInput struct {
	RemoteStoreName string
	RemotePath      string
	LocalPath       string
	RemoteDsName    string
	DsPathKey       string
	DsDataPathKey   string
}

func (im *IOManager) CopyFileToRemote(input CopyFileToRemoteInput) error {
	storeName := input.RemoteStoreName
	path := input.RemotePath
	datapath := ""
	if storeName == "" {
		//get store name from datasource and use datasource semantics
		ds, err := im.GetDataSource(GetDsInput{DataSourceOutput, input.RemoteDsName})
		if err != nil {
			return err
		}
		storeName = ds.StoreName
		path = ds.Paths[input.DsPathKey]
		if input.DsDataPathKey != "" {
			datapath = ds.DataPaths[input.DsDataPathKey]
		}

	}

	store, err := im.GetStore(storeName)
	if err != nil {
		return err
	}

	if writer, ok := store.Session.(StoreWriter); ok {
		reader, err := os.Open(input.LocalPath)
		if err != nil {
			return err
		}

		_, err = writer.Put(reader, path, datapath)
		return err
	}

	return fmt.Errorf("Data Store %s session does not implement a StoreWriter", store.Name)
}

func (im *IOManager) CopyFileToRemoteOld(dsDestName string, pathkey string, dataPathKey string, localPath string) error {
	ds, err := im.GetDataSource(GetDsInput{DataSourceOutput, dsDestName})
	if err != nil {
		return err
	}

	store, err := im.GetStore(ds.StoreName)
	if err != nil {
		return err
	}

	if writer, ok := store.Session.(StoreWriter); ok {
		reader, err := os.Open(localPath)
		if err != nil {
			return err
		}

		path := ds.Paths[pathkey]
		datapath := ""
		if dataPathKey != "" {
			datapath = ds.DataPaths[dataPathKey]
		}

		_, err = writer.Put(reader, path, datapath)
		return err
	}

	return fmt.Errorf("Data Store %s session does not implement a StoreWriter", ds.StoreName)
}

/*
func (im *IOManager) fileReader(ds DataSource, pathkey string) (io.ReadCloser, error) {
	store, err := GetStoreAs[FileDataStore](im, ds.StoreName)
	if err != nil {
		return nil, err
	}

	reader, err := store.Get(ds.Paths[pathkey])

	return reader, err
}
*/

/*
func (im *IOManager) GetReader(dataSourceName string, filePathKey string, dataPathKey string) (io.Reader, error) {
	ds,err:=im.GetDataSource(GetDsInput{DataSourceInput,dataSourceName})
	if err!=nil{
		return nil,err
	}
	ds.

}
*/

func GetStoreAs[T any](mgr *IOManager, name string) (T, error) {
	for _, s := range mgr.Stores {
		if s.Name == name {
			if t, ok := s.Session.(T); ok {
				return t, nil
			} else {
				return t, errors.New("Invalid Store Type")
			}
		}
	}
	var t T
	return t, errors.New(fmt.Sprintf("Session %s does not exist.\n", name))
}
