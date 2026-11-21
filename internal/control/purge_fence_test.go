package control

import (
	"context"
	"testing"
	"time"
)

// A durable running job is not a cancellable pending deletion, including after
// a different Store opens the same control cell or its apparent age expires.
func TestPurgeFenceRejectsResurrection(t *testing.T) {
	for _, state := range []string{"running", "uncertain"} {
		for _, operation := range []string{"app", "deploy", "resource", "delete", "finish", "fail"} {
			t.Run(state+"/"+operation, func(t *testing.T) {
				ctx := context.Background()
				s := newStore(t, false)
				if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1"}, "test"); err != nil {
					t.Fatal(err)
				}
				if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
					t.Fatal(err)
				}
				c, err := s.cell(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.DB.ExecContext(ctx, `UPDATE purges SET state=? WHERE ns='acme'`, state); err != nil {
					t.Fatal(err)
				}
				other := New(s.cs, nil)
				other.now = func() time.Time { return time.Now().Add(365 * 24 * time.Hour) }
				switch operation {
				case "app":
					_, err = other.CreateApp(ctx, "acme", "test")
				case "deploy":
					_, err = other.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha2"}, "test")
				case "resource":
					_, err = other.CreateResource(ctx, "acme", "d1", "new", "acme/__d1__/new", "test")
				case "delete":
					err = other.DeleteApp(ctx, "acme", "test")
				case "finish":
					err = other.FinishPurge(ctx, "acme", "")
				case "fail":
					err = other.FailPurge(ctx, "acme", "", "retry")
				}
				if err == nil {
					t.Fatalf("%s accepted while purge is %s", operation, state)
				}
				var got string
				if err := c.DB.QueryRowContext(ctx, `SELECT state FROM purges WHERE ns='acme' AND worker=''`).Scan(&got); err != nil || got != state {
					t.Fatalf("fence changed: state=%q err=%v", got, err)
				}
			})
		}
	}
}
