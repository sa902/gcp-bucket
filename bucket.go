package gcpbucket

import (
	"go.k6.io/k6/js/modules"

	"context"
	"encoding/json"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

func init() {
	modules.Register("k6/x/gcp-bucket", new(GCPBucket))
}

type User struct {
	Name  string `bigquery:"name"`
	Age   int    `bigquery:"age"`
	Email string `bigquery:"email"`
}

type GCPBucket struct {
	TestString string
}

func (g *GCPBucket) UploadToBucket(bucketName, destObjectName string, jsonObject interface{}) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	jsonData, err := json.Marshal(jsonObject)
	if err != nil {
		return err
	}

	bucket := client.Bucket(bucketName)
	obj := bucket.Object(destObjectName)

	wc := obj.NewWriter(ctx)

	if _, err = wc.Write(jsonData); err != nil {
		return err
	}

	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

func (g *GCPBucket) Test(s string) {
	g.TestString = s

}

type Row struct {
	Duration    float64 `json:"duration"`
	Sending     float64 `json:"sending"`
	Waiting     float64 `json:"waiting"`
	Receiving   float64 `json:"receiving"`
	Parameters  string  `json:"parameters"`
	IterationID string  `json:"iteration_id"`
}

func (g *GCPBucket) UploadToBigQuery(projId, datasetName, tableName, data string) error {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projId)
	if err != nil {
		return err
	}
	defer client.Close()

	var row Row

	err = json.Unmarshal([]byte(data), &row)
	if err != nil {
		return err
	}

	table := client.Dataset(datasetName).Table(tableName)

	inserter := table.Inserter()
	inserter.SkipInvalidRows = true
	inserter.IgnoreUnknownValues = true

	if err := inserter.Put(ctx, row); err != nil {
		return err
	}
	return nil
}
