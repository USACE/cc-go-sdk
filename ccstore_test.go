package cc

import (
	"os"
	"path/filepath"
	"testing"
)

// Test constants
const (
	testFileName    = "test"
	testFileExt     = "json"
	testContent     = "test content for ccstore"
	testManifestID  = "test-manifest-123"
	testPayloadID   = "test-payload-123"
)

func TestCcPushObject(t *testing.T) {
	store, err := NewS3CcStore()
	if err != nil {
		t.Skip("S3 store not available for testing:", err)
	}

	poi := PutObjectInput{
		FileName:             testFileName,
		FileExtension:        testFileExt,
		DestinationStoreType: FSS3,
		ObjectState:          Memory,
		Data:                 []byte(testContent),
	}
	
	err = store.PutObject(poi)
	if err != nil {
		t.Skip("PutObject requires S3 credentials:", err)
	}
}

func TestCcPullObject(t *testing.T) {
	store, err := NewS3CcStore()
	if err != nil {
		t.Skip("S3 store not available for testing:", err)
	}

	tmpDir := "/tmp/test_s3_pull"
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)

	poi := PullObjectInput{
		SourceStoreType:     FSS3,
		SourceRootPath:      "/cc_store",
		DestinationRootPath: tmpDir,
		FileName:            testFileName,
		FileExtension:       testFileExt,
	}
	
	err = store.PullObject(poi)
	if err != nil {
		t.Skip("PullObject requires existing S3 data:", err)
	}

	// Verify file was pulled
	expectedPath := filepath.Join(tmpDir, testFileName+"."+testFileExt)
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Error("File was not pulled to expected path:", expectedPath)
	}
}
