package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// executeRoot runs the cobra tree with captured I/O, the way main does.
func executeRoot(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	root := newRoot()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestNormalizeLegacyLongFlags(t *testing.T) {
	root := newRoot()
	in := []string{"deploy", "acme", "api", "-bundle-sha", "x", "-config", "f", "-c", "g", "--", "-config", "keep"}
	got := normalizeLegacyLongFlags(root, in)
	want := []string{"deploy", "acme", "api", "--bundle-sha", "x", "--config", "f", "-c", "g", "--", "-config", "keep"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("normalize = %v, want %v", got, want)
	}
}

func TestRootVersionFlag(t *testing.T) {
	out, _, err := executeRoot(t, "--version")
	if err != nil {
		t.Fatal(err)
	}
	if out != "cellhive "+version+"\n" {
		t.Fatalf("version output = %q", out)
	}
}

func TestRootHelpListsNativeAndHidesCompatAliases(t *testing.T) {
	out, _, err := executeRoot(t, "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"deploy", "app", "kv", "d1", "queue", "wrangler"} {
		if !strings.Contains(out, "\n  "+name+" ") {
			t.Errorf("root help missing native command %q", name)
		}
	}
	for _, name := range []string{"versions", "deployments", "triggers", "queues", "workflows"} {
		if strings.Contains(out, "\n  "+name+" ") {
			t.Errorf("root help should hide compatibility alias %q", name)
		}
	}
}

func TestRootUnknownCommandIsReported(t *testing.T) {
	_, _, err := executeRoot(t, "nope")
	if err == nil || !isUnknownCommand(err) {
		t.Fatalf("unknown command error = %v", err)
	}
}

func TestRootNoArgsKeepsExitCodeTwo(t *testing.T) {
	out, _, err := executeRoot(t)
	if !errors.Is(err, errNoCommand) {
		t.Fatalf("no-arg error = %v, want errNoCommand", err)
	}
	if !strings.Contains(out, "Available Commands:") {
		t.Fatalf("no-arg help was not printed: %q", out)
	}
}

func TestLegacyCommandLoneHelpUsesCobra(t *testing.T) {
	out, _, err := executeRoot(t, "deploy", "--help")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "deploy a worker version") {
		t.Fatalf("deploy help = %q", out)
	}
	out, _, err = executeRoot(t, "queue", "consumer", "--help")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "manage queue consumers") {
		t.Fatalf("queue consumer help = %q", out)
	}
}

func TestRootWranglerAndQueueConsumerAreRouted(t *testing.T) {
	if _, _, err := executeRoot(t, "wrangler"); err == nil || !strings.Contains(err.Error(), "usage: cellhive wrangler") {
		t.Fatalf("wrangler without args = %v", err)
	}
	if _, _, err := executeRoot(t, "queue", "consumer"); err == nil || !strings.Contains(err.Error(), "usage: cellhive queue consumer") {
		t.Fatalf("queue consumer without args = %v", err)
	}
}
