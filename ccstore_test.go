package cc

import (
	"testing"
)

func TestCcPushObject(t *testing.T) {
	store, err := NewS3CcStore()
	if err != nil {
		t.Fatal(err)
	}
	poi := PutObjectInput{
		FileName:             "test",
		FileExtension:        "json",
		DestinationStoreType: S3,
		ObjectState:          LocalDisk,
		Data:                 []byte{},
		SourcePath:           "/data",
		DestPath:             "/cc_store",
	}
	err = store.PutObject(poi)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCcPullObject(t *testing.T) {
	store, err := NewS3CcStore()
	if err != nil {
		t.Fatal(err)
	}
	poi := PullObjectInput{
		SourceStoreType:     S3,
		SourceRootPath:      "/cc_store",
		DestinationRootPath: "/data",
		FileName:            "test",
		FileExtension:       ".json",
	}
	err = store.PullObject(poi)
	if err != nil {
		t.Fatal(err)
	}
}
