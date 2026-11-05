package control

import (
	"context"
	"database/sql"
	"testing"
)

func TestProjectionBatchesWorkersAndClassesAtScale(t *testing.T) {
	s := newStore(t, false)
	ctx := context.Background()
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 1000
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<?)
 INSERT INTO workers(ns,name,active,storage_id,created_ms) SELECT 'acme','w'||x,1,'s'||x,1 FROM n`, workers-1); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<?)
 INSERT INTO versions(ns,worker,number,bundle_sha,bindings,consumers,created_ms)
 SELECT 'acme','w'||x,1,'sha'||x,
        json_array(json_object('type','workflow','name','wf'||x,'class_name','WF')),
        json_array(json_object('queue','q'||x,'max_batch_size',10)),1 FROM n`, workers-1); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<100)
 INSERT INTO do_classes(ns,worker,code_class,storage_class,deleted) SELECT 'acme','w'||x,'Code','Stored',0 FROM n`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO routes(host,path,ns,worker) VALUES('app.test','/','acme','w0')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	p, err := s.Projection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Apps) != 1 || len(p.Apps[0].Workers) != workers {
		t.Fatalf("apps/workers = %d/%d", len(p.Apps), len(p.Apps[0].Workers))
	}
	first := p.Apps[0].Workers[0]
	if first.Worker != "w0" || first.Version.BundleSHA != "sha0" || len(first.Version.Bindings) != 1 || len(first.Version.Consumers) != 1 {
		t.Fatalf("first worker = %+v", first)
	}
	if first.ClassStorage["Code"] != "Stored" {
		t.Fatalf("class aliases = %v", first.ClassStorage)
	}
	if last := p.Apps[0].Workers[workers-1]; last.Worker != "w999" || last.Version.BundleSHA != "sha999" {
		t.Fatalf("last worker = %+v", last)
	}
	if got, ok := p.WorkflowTargetFor("acme", "wf999"); !ok || got.Name != "wf999" || got.Worker != "w999" {
		t.Fatalf("workflow target = %+v", got)
	}
	if got := p.QueueTargets(); len(got) != workers || got[workers-1].Queue != "q999" {
		t.Fatalf("queue targets = %d", len(got))
	}
	if len(p.Apps[0].Routes) != 1 || p.Apps[0].Routes[0].Host != "app.test" {
		t.Fatalf("routes = %+v", p.Apps[0].Routes)
	}
}
