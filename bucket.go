package gcpbucket

import (
	"go.k6.io/k6/js/modules"

	"context"
	"encoding/json"

	"cloud.google.com/go/storage"
)

func init() {
	modules.Register("k6/x/gcp-bucket", new(GCPBucket))
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
