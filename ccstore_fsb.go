package cc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FSBCcStore implements the CcStore interface for local file system storage
type FSBCcStore struct {
	localRootPath  string
	remoteRootPath string
	manifestId     string
	payloadId      string
	storeType      StoreType
}

// NewFSBCcStore creates a new FSB CcStore instance
func NewFSBCcStore(manifestArgs ...string) (CcStore, error) {
	var manifestId string
	var payloadId string
	if len(manifestArgs) > 1 {
		manifestId = manifestArgs[0]
		payloadId = manifestArgs[1]
	} else {
		manifestId = os.Getenv(CcManifestId)
		payloadId = os.Getenv(CcPayloadId)
	}

	rootPath := os.Getenv(FsbRootPath)
	if rootPath == "" {
		rootPath = "/data" // default local root path
	}

	// Ensure the root directory exists
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	return &FSBCcStore{localRootPath, rootPath, manifestId, payloadId, FSB}, nil
}

// HandlesDataStoreType determines if a datasource is handled by this store
func (fs *FSBCcStore) HandlesDataStoreType(storeType StoreType) bool {
	return fs.storeType == storeType
}

// RootPath provides access to the local root path
func (fs *FSBCcStore) RootPath() string {
	return fs.localRootPath
}

// PutObject stores a file in the local file system
func (fs *FSBCcStore) PutObject(poi PutObjectInput) error {
	destPath := filepath.Join(fs.remoteRootPath, fs.manifestId, fmt.Sprintf("%s.%s", poi.FileName, poi.FileExtension))

	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	var data []byte
	var err error

	switch poi.ObjectState {
	case LocalDisk:
		sourcePath := filepath.Join(fs.localRootPath, fmt.Sprintf("%s.%s", poi.FileName, poi.FileExtension))
		data, err = os.ReadFile(sourcePath)
		if err != nil {
			return fmt.Errorf("failed to read source file: %w", err)
		}
	case Memory:
		data = poi.Data
	default:
		return errors.New("unsupported object state for FSB store")
	}

	return os.WriteFile(destPath, data, 0644)
}

// GetObject retrieves a file from the local file system
func (fs *FSBCcStore) GetObject(input GetObjectInput) ([]byte, error) {
	filePath := filepath.Join(input.SourceRootPath, fs.manifestId, fmt.Sprintf("%s.%s", input.FileName, input.FileExtension))

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

// GetPayload retrieves the payload from the local file system
func (fs *FSBCcStore) GetPayload() (Payload, error) {
	var payload Payload

	filePath := filepath.Join(fs.remoteRootPath, fs.payloadId, payloadFileName)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return payload, fmt.Errorf("failed to read payload file: %w", err)
	}

	err = json.Unmarshal(data, &payload)
	if err != nil {
		return payload, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return payload, nil
}

// SetPayload stores a payload in the local file system
func (fs *FSBCcStore) SetPayload(p Payload) error {
	filePath := filepath.Join(fs.remoteRootPath, fs.payloadId, payloadFileName)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	_, shouldFormat := os.LookupEnv(CcPayloadFormatted)
	var data []byte
	var err error

	if shouldFormat {
		data, err = json.MarshalIndent(p, "", "  ")
	} else {
		data, err = json.Marshal(p)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	return os.WriteFile(filePath, data, 0644)
}

// PullObject copies a file from the remote location to local directory
func (fs *FSBCcStore) PullObject(input PullObjectInput) error {
	sourcePath := filepath.Join(input.SourceRootPath, fs.manifestId, fmt.Sprintf("%s.%s", input.FileName, input.FileExtension))
	destPath := filepath.Join(input.DestinationRootPath, fmt.Sprintf("%s.%s", input.FileName, input.FileExtension))

	// Create destination directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Open source file
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	// Create destination file
	destFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()

	// Copy the file
	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	return nil
}
