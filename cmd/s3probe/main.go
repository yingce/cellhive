// Command s3probe precisely checks the four object-store properties the cell
// protocol requires: conditional create, reject-create, CAS update,
// reject-stale, plus ranged read. It prints one line per property.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"cellhive/internal/bucket"
)

func main() {
	endpoint := flag.String("endpoint", "", "S3 endpoint")
	region := flag.String("region", "us-east-1", "region")
	access := flag.String("access", "", "access key")
	secret := flag.String("secret", "", "secret key")
	name := flag.String("bucket", "", "bucket")
	pathStyle := flag.Bool("path-style", false, "use path-style addressing")
	debug := flag.Bool("debug", false, "log signed requests/responses")
	flag.Parse()

	ctx := context.Background()
	b, err := bucket.NewS3Bucket(ctx, bucket.S3Options{
		Endpoint: *endpoint, Region: *region, AccessKey: *access, SecretKey: *secret,
		Bucket: *name, PathStyle: *pathStyle, Debug: *debug,
	})
	if err != nil {
		fatal("client", err)
	}

	base := fmt.Sprintf("probe/%d", time.Now().UnixNano())
	key := base + "/cap"

	// 1. conditional create on absent object
	e1, err := b.ConditionalCreate(ctx, key, []byte("one"))
	report("conditional create (expect success)", err == nil, err)

	// 2. conditional create on existing object (expect ErrPrecondition)
	_, err = b.ConditionalCreate(ctx, key, []byte("two"))
	report("reject-create (expect precondition failed)", errors.Is(err, bucket.ErrPrecondition), err)

	// 3. CAS with correct etag (expect success)
	_, err = b.CAS(ctx, key, []byte("three"), e1)
	report("CAS update (expect success)", err == nil, err)

	// 4. CAS with stale etag (expect ErrPrecondition)
	_, err = b.CAS(ctx, key, []byte("four"), e1)
	report("reject-stale (expect precondition failed)", errors.Is(err, bucket.ErrPrecondition), err)

	// 5. ranged read
	_, _ = b.Put(ctx, base+"/r", []byte("0123456789"))
	got, _, err := b.RangedGet(ctx, base+"/r", 2, 4)
	ok := err == nil && bytes.Equal(got, []byte("2345"))
	report("ranged read (expect 2345)", ok, err)

	_ = b.Delete(ctx, key)
	_ = b.Delete(ctx, base+"/r")
}

func report(label string, ok bool, err error) {
	status := "OK"
	if !ok {
		status = "FAIL"
	}
	if err != nil {
		fmt.Printf("[%s] %-45s err=%v\n", status, label, err)
	} else {
		fmt.Printf("[%s] %s\n", status, label)
	}
}

func fatal(what string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
	os.Exit(1)
}
