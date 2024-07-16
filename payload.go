package cc

import (
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
	Actions []Action
}

type Action struct {
	IOManager
	Type        string `json:"type"`
	Description string `json:"desc"`
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

func (im *IOManager) CopyFileToLocal(dsName string, pathkey string, localPath string) error {
	ds, err := im.GetDataSource(GetDsInput{DataSourceInput, dsName})
	if err != nil {
		return err
	}
	reader, err := im.fileReader(ds, pathkey)
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

func (im *IOManager) CopyFileToRemote(dsDestName string, pathkey string, localPath string) error {
	ds, err := im.GetDataSource(GetDsInput{DataSourceOutput, dsDestName})
	if err != nil {
		return err
	}
	store, err := GetStoreSession[FileDataStore](im, ds.StoreName)
	if err != nil {
		return err
	}

	reader, err := os.Open(localPath)
	if err != nil {
		return err
	}

	return store.Put(reader, localPath)
}

func (im *IOManager) fileReader(ds DataSource, pathkey string) (io.ReadCloser, error) {
	store, err := GetStoreSession[FileDataStore](im, ds.StoreName)
	if err != nil {
		return nil, err
	}

	reader, err := store.Get(ds.Paths[pathkey])

	return reader, err
}
