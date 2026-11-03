package auth

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestPrincipalNamespaceScopes(t *testing.T) {
	for _, tt := range []struct {
		name string
		p    Principal
		ns   string
		want bool
	}{
		{"empty", Principal{}, "app", false},
		{"exact", Principal{NS: []string{"app"}}, "app", true},
		{"exact boundary", Principal{NS: []string{"app"}}, "apple", false},
		{"prefix", Principal{NS: []string{"vwork-*"}}, "vwork-42", true},
		{"prefix boundary", Principal{NS: []string{"vwork-*"}}, "vwork", false},
		{"prefix empty suffix", Principal{NS: []string{"vwork-*"}}, "vwork-", true},
		{"wildcard", Principal{NS: []string{"*"}}, "app", true},
		{"multiple", Principal{NS: []string{"other", "vwork-*"}}, "vwork-42", true},
		{"case sensitive", Principal{NS: []string{"app"}}, "App", false},
		{"all business", Principal{All: true}, "app", true},
		{"invalid grant fails closed", Principal{NS: []string{"*", "bad/*"}}, "app", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Allows(tt.ns); got != tt.want {
				t.Fatalf("Allows(%q)=%v want %v", tt.ns, got, tt.want)
			}
		})
	}
}

func TestPrincipalRejectsReservedAndInvalidTargets(t *testing.T) {
	for _, ns := range []string{"__platform__", "__system", "__", "", ".", "..", "a..b", "app*", "*", "a/b", "a\\b", "a b", "a\n", "租户"} {
		for _, p := range []Principal{{All: true}, {NS: []string{"*"}}, {NS: []string{ns}}} {
			if p.Allows(ns) {
				t.Errorf("%+v allows invalid business namespace %q", p, ns)
			}
		}
	}
}

func TestPrincipalAuditFieldsRoundTrip(t *testing.T) {
	p := Principal{Sub: "caller", Kind: "namespace", NS: []string{"app-*"}, OnBehalfOf: "operator", RequestID: "req-1"}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var got Principal
	if err = json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, got) {
		t.Fatalf("got %+v want %+v", got, p)
	}
}
