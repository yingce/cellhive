// Command recoververify runs node-log recovery for a dead node (docs/cell-protocol.md §6):
// it lists the node's unsealed sessions, collects acknowledged-but-unuploaded
// segments from their followers, uploads them to the bucket, and seals the
// sessions. Sealed sessions take the fast path.
package main

import (
	"cellhive/internal/config"
	"context"
	"flag"
	"fmt"
	"os"

	"cellhive/internal/bucket"
	"cellhive/internal/nodelog"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
)

func main() {
	bucketDir := flag.String("bucket", "", "bucket directory")
	node := flag.String("node", "", "dead node id")
	token := flag.String("token", config.DeriveCredentials(config.LoadRootKey()).Internal, "internal-role token (default derived from CELLHIVE_ROOT_KEY)")
	flag.Parse()
	if *bucketDir == "" || *node == "" {
		fmt.Fprintln(os.Stderr, "bucket and node are required")
		os.Exit(2)
	}
	b, err := bucket.NewFSBucket(*bucketDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bucket:", err)
		os.Exit(1)
	}
	nl := nodelog.New(b, *node)
	rec := recovery.New(replica.New(b), peer.NewHTTPTransport(*token, nil))
	segments, sessions, err := rec.RecoverNode(context.Background(), nl, *node)
	if err != nil {
		fmt.Fprintln(os.Stderr, "recover:", err)
		os.Exit(1)
	}
	fmt.Printf("{\"node\":%q,\"sessions\":%d,\"segments\":%d}\n", *node, sessions, segments)
}
