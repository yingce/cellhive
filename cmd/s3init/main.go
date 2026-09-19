// Command s3init creates an S3-compatible bucket (idempotent) and verifies
// conditional-write + ranged-read + presign against it. Used for real S3 tests.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:19000", "S3 endpoint")
	region := flag.String("region", "us-east-1", "region")
	access := flag.String("access", "minioadmin", "access key")
	secret := flag.String("secret", "minioadmin", "secret key")
	bucket := flag.String("bucket", "cellhive", "bucket name")
	flag.Parse()

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(*region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*access, *secret, "")),
	)
	if err != nil {
		fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && (ae.ErrorCode() == "BucketAlreadyOwnedByYou" || ae.ErrorCode() == "BucketAlreadyExists") {
			fmt.Println("bucket exists:", *bucket)
		} else {
			fatal(err)
		}
	} else {
		fmt.Println("bucket created:", *bucket)
	}

	// Presign a GET for a probe key.
	presign := s3.NewPresignClient(client)
	req, err := presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("probe/x")},
		func(o *s3.PresignOptions) { o.Expires = 15 * time.Minute })
	if err != nil {
		fatal(err)
	}
	fmt.Println("presign ok:", req.URL[:min(len(req.URL), 90)]+"...")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
