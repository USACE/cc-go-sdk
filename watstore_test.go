package wat

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"testing"

	"github.com/google/uuid"
)

func TestWatPushObject(t *testing.T) {
	store, err := NewS3WatStore()
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
		DestPath:             "/wat_store",
	}
	err = store.PutObject(poi)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWatPullObject(t *testing.T) {
	store, err := NewS3WatStore()
	if err != nil {
		t.Fatal(err)
	}
	poi := PullObjectInput{
		SourceStoreType:     S3,
		SourceRootPath:      "/wat_store",
		DestinationRootPath: "/data",
		FileName:            "test",
		FileExtension:       ".json",
	}
	err = store.PullObject(poi)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWatGetObject(t *testing.T) {
	store, err := NewS3WatStore()
	if err != nil {
		t.Fatal(err)
	}
	goi := GetObjectInput{
		SourceStoreType: S3,
		SourceRootPath:  "/wat_store",
		FileName:        "test",
		FileExtension:   ".json",
	}
	data, err := store.GetObject(goi)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(data))
}
func payload() Payload {
	payload := Payload{
		map[string]interface{}{
			"1": 1,
			"2": "2",
			"3": map[string]string{
				"3a": "three-a",
				"3b": "three-b",
			},
		},
		[]DataSource{
			{
				Name: "Input1",
				//DataType:  fileDataType,
				StoreType: S3,
				EnvPrefix: "MMC_TIMING",
				Paths:     []string{"mmc_timing_test"},
				Parameters: map[string]string{
					"P1": "TEST1",
					"P2": "TEST2",
				},
			},
		},
		[]DataSource{
			{
				Name: "Output1",
				//DataType:  fileDataType,
				StoreType: S3,
				EnvPrefix: "MMC_TIMING",
				Paths:     []string{"mmc_timing_test"},
				Parameters: map[string]string{
					"OP1": "OTEST1",
					"OP2": "OTEST2",
				},
			},
		},
	}
	return payload
}
func TestWatSetPayload(t *testing.T) {
	store, err := NewS3WatStore()
	if err != nil {
		t.Fatal(err)
	}
	payload := payload()
	err = store.SetPayload(payload)
	if err != nil {
		t.Fatal(err)
	}

}

func TestWatSetPayload2(t *testing.T) {
	store, err := NewS3WatStore()
	if err != nil {
		t.Fatal(err)
	}
	payload := Payload{
		map[string]interface{}{
			"BreachTime":   "05FEB2099 01:25:00",
			"BreachPlan":   "BirchLakeDam.p10.hdf",
			"NoBreachPlan": "BirchLakeDam.p09.hdf",
			"Scenario":     "MHZ",
			"S3Root":       "/adrian_christopher_test/Birch_Lake_Dam",
			"Delta":        2,
		},
		[]DataSource{},
		[]DataSource{},
	}
	err = store.SetPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
}
func TestWritePayloadToJson(t *testing.T) {
	p := payload()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	ioutil.WriteFile("payload.json", data, fs.ModeAppend)
}
func TestWatSetPayload3(t *testing.T) {
	store, err := NewS3WatStore()
	if err != nil {
		t.Fatal(err)
	}
	sampleId := uuid.New()
	payload := Payload{
		map[string]interface{}{
			"BreachTime": "05FEB2099 01:25:00",
			"Scenario":   "MHZ",
			"Delta":      2,
		},
		[]DataSource{
			{
				Name:      "BreachPlan",
				StoreType: S3,
				Paths:     []string{"/adrian_christopher_test/Birch_Lake_Dam/BirchLakeDam.p10.hdf"},
			},
			{
				Name:      "BreachPlan",
				StoreType: S3,
				Paths:     []string{"/adrian_christopher_test/Birch_Lake_Dam/BirchLakeDam.p09.hdf"},
			},
		},
		[]DataSource{
			{
				Name:      "log",
				StoreType: S3,
				EnvPrefix: "MMC",
				Paths:     []string{"/adrian_christopher_test/Birch_Lake_Dam/Timing/BirchLakeDam_%s.log"},
			},
			{
				Name:      "timing",
				ID:        &sampleId,
				StoreType: S3,
				EnvPrefix: "MMC",
				Paths:     []string{"/adrian_christopher_test/Birch_Lake_Dam/Timing/BirchLakeDam_%s.gpkg"},
			},
		},
	}
	err = store.SetPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWatSetPayloadM(t *testing.T) {
	store, err := NewS3WatStore()
	if err != nil {
		t.Fatal(err)
	}
	payload := Payload{
		map[string]interface{}{
			"BreachTime":   "05FEB2099 01:25:00",
			"BreachPlan":   "BirchLakeDam.p10.hdf",
			"NoBreachPlan": "BirchLakeDam.p09.hdf",
			"Scenario":     "MHZ",
			"S3Root":       "/adrian_christopher_test/Birch_Lake_Dam",
			"Delta":        2,
			"model": map[string]interface{}{
				"name":         "Model1",
				"alternatives": []string{"breach", "nobreach"},
			},
		},
		[]DataSource{},
		[]DataSource{},
	}
	err = store.SetPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
}
