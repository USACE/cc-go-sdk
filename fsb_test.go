package cc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestConfig represents test configuration loaded from JSON
type TestConfig struct {
	FSBTest struct {
		StoreType  string `json:"store_type"`
		RootPath   string `json:"root_path"`
		ManifestID string `json:"manifest_id"`
		PayloadID  string `json:"payload_id"`
	} `json:"fsb_test"`
	IntegrationTest struct {
		StoreType  string `json:"store_type"`
		RootPath   string `json:"root_path"`
		ManifestID string `json:"manifest_id"`
		PayloadID  string `json:"payload_id"`
	} `json:"integration_test"`
}

// loadTestConfig loads test configuration from JSON file
func loadTestConfig(t *testing.T) TestConfig {
	data, err := os.ReadFile("testdata/test_config.json")
	if err != nil {
		t.Fatalf("Failed to read test config: %v", err)
	}

	var config TestConfig
	err = json.Unmarshal(data, &config)
	if err != nil {
		t.Fatalf("Failed to unmarshal test config: %v", err)
	}

	return config
}

// loadTestPayload loads test payload from JSON file
func loadTestPayload(t *testing.T) []byte {
	data, err := os.ReadFile("testdata/test_payload.json")
	if err != nil {
		t.Fatalf("Failed to read test payload: %v", err)
	}
	return data
}

// setupTestEnvironment sets up environment variables for testing
func setupTestEnvironment(storeType, rootPath, manifestID, payloadID string) {
	os.Setenv("CC_STORE_TYPE", storeType)
	if rootPath != "" {
		os.Setenv("FSB_ROOT_PATH", rootPath)
	}
	os.Setenv("CC_MANIFEST_ID", manifestID)
	os.Setenv("CC_PAYLOAD_ID", payloadID)
	os.Setenv("CC_EVENT_IDENTIFIER", "test-event")
}

// cleanupTestEnvironment cleans up test environment
func cleanupTestEnvironment(testPaths []string) {
	// Clean environment variables
	envVars := []string{"CC_STORE_TYPE", "FSB_ROOT_PATH", "CC_MANIFEST_ID", "CC_PAYLOAD_ID", "CC_EVENT_IDENTIFIER"}
	for _, env := range envVars {
		os.Unsetenv(env)
	}

	// Clean up test directories
	for _, path := range testPaths {
		os.RemoveAll(path)
	}
}

func TestFSBCcStore(t *testing.T) {
	config := loadTestConfig(t)
	fsbConfig := config.FSBTest

	setupTestEnvironment(fsbConfig.StoreType, fsbConfig.RootPath, fsbConfig.ManifestID, fsbConfig.PayloadID)
	defer cleanupTestEnvironment([]string{fsbConfig.RootPath})

	store, err := NewCcStore()
	if err != nil {
		t.Fatalf("Failed to create FSB store: %v", err)
	}

	if !store.HandlesDataStoreType(FSB) {
		t.Error("FSB store should handle FSB data store type")
	}

	if store.HandlesDataStoreType(FSS3) {
		t.Error("FSB store should not handle S3 data store type")
	}
}

func TestFSBIntegration(t *testing.T) {
	config := loadTestConfig(t)
	testConfig := config.IntegrationTest
	payloadData := loadTestPayload(t)

	setupTestEnvironment(testConfig.StoreType, testConfig.RootPath, testConfig.ManifestID, testConfig.PayloadID)
	defer cleanupTestEnvironment([]string{testConfig.RootPath})

	// Create test directory structure and payload
	payloadDir := filepath.Join(testConfig.RootPath, testConfig.PayloadID)
	os.MkdirAll(payloadDir, 0755)

	payloadFile := filepath.Join(payloadDir, "payload")
	err := os.WriteFile(payloadFile, payloadData, 0644)
	if err != nil {
		t.Fatalf("Failed to create test payload: %v", err)
	}

	// Test store creation
	store, err := NewCcStore()
	if err != nil {
		t.Fatalf("NewCcStore failed: %v", err)
	}

	if !store.HandlesDataStoreType(FSB) {
		t.Error("Store should handle FSB type")
	}

	// Test payload retrieval
	retrievedPayload, err := store.GetPayload()
	if err != nil {
		t.Fatalf("GetPayload failed: %v", err)
	}

	if len(retrievedPayload.Attributes) == 0 {
		t.Error("Payload should have attributes")
	}

	// Test InitPluginManager
	pm, err := InitPluginManager()
	if err != nil {
		t.Fatalf("InitPluginManager failed: %v", err)
	}

	if pm.ccStore == nil {
		t.Fatal("PluginManager should have a store")
	}

	// Test object operations
	testData := []byte("integration test data")
	putInput := PutObjectInput{
		FileName:      "integration",
		FileExtension: "txt",
		ObjectState:   Memory,
		Data:          testData,
	}

	err = store.PutObject(putInput)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify file exists
	expectedFile := filepath.Join(testConfig.RootPath, testConfig.ManifestID, "integration.txt")
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Error("File should exist after PutObject")
	}

	// Test retrieval
	getInput := GetObjectInput{
		SourceRootPath: testConfig.RootPath,
		FileName:       "integration",
		FileExtension:  "txt",
	}

	retrievedData, err := store.GetObject(getInput)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}

	if string(retrievedData) != string(testData) {
		t.Error("Retrieved data doesn't match original data")
	}

	t.Log("✅ FSB integration test passed!")
}

func TestStoreSelection(t *testing.T) {
	testCases := []struct {
		name      string
		storeType string
		expected  StoreType
		shouldErr bool
	}{
		{"FSB Selection", "FS", FSB, false},
		{"S3 Selection", "S3", FSS3, false},
		{"Default Selection", "", FSS3, false}, // Should default to S3
		{"Invalid Selection", "INVALID", FSB, true}, // Should error
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean slate
			os.Unsetenv("CC_STORE_TYPE")
			os.Unsetenv("FSB_ROOT_PATH")

			if tc.storeType != "" {
				os.Setenv("CC_STORE_TYPE", tc.storeType)
			}

			if tc.expected == FSB {
				os.Setenv("FSB_ROOT_PATH", "/tmp/cc-store-selection-test")
			}

			store, err := NewCcStore()
			if tc.shouldErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil && tc.expected == FSS3 {
				// S3 might fail without credentials, that's okay for this test
				t.Logf("Expected S3 failure: %v", err)
				return
			}

			if err != nil {
				t.Fatalf("Failed to create store: %v", err)
			}

			if !store.HandlesDataStoreType(tc.expected) {
				t.Errorf("Expected store to handle %v, but it doesn't", tc.expected)
			}

			// Cleanup
			os.RemoveAll("/tmp/cc-store-selection-test")
		})
	}
}