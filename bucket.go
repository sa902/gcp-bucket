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
}

func (g *GCPBucket) UploadToBucket(bucketName, destObjectName string, jsonObject interface{}) error {
	// Create context
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

func (g *GCPBucket) UploadToBigQuery(projId, datasetName, tableName string) error {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projId)
	if err != nil {
		return err
	}
	defer client.Close()

	users := []*User{
		{Name: "Alice", Age: 30, Email: "alice@example.com"},
		// {Name: "Bob", Age: 35, Email: "bob@example.com", SignupAt: time.Now()},
	}

	table := client.Dataset(datasetName).Table(tableName)

	inserter := table.Inserter()
	inserter.SkipInvalidRows = true     // Optional: skip bad rows
	inserter.IgnoreUnknownValues = true // Optional: ignore unknown fields

	if err := inserter.Put(ctx, users); err != nil {
		return err
	}
	return nil
}
