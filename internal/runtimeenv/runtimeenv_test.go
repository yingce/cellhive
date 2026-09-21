package runtimeenv

import (
	"slices"
	"testing"
)

func TestBuildIsSortedAndRejectsInvalidInput(t *testing.T) {
	got, err := Build(map[string]string{"B": "2", "A": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal([]string{"A=1", "B=2"}, got) {
		t.Fatalf("env=%q", got)
	}
	for name, values := range map[string]map[string]string{
		"equals in name": {"BAD=NAME": "x"},
		"lowercase name": {"bad": "x"},
		"nul in value":   {"GOOD": "x\x00y"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Build(values); err == nil {
				t.Fatal("wanted validation error")
			}
		})
	}
}

func TestBuildKeepsPresentEmptyValues(t *testing.T) {
	got, err := Build(map[string]string{"OPTIONAL": ""})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal([]string{"OPTIONAL="}, got) {
		t.Fatalf("env=%q", got)
	}
}

func TestRequireDistinguishesMissingFromEmpty(t *testing.T) {
	if err := Require([]string{"OPTIONAL="}, "OPTIONAL"); err != nil {
		t.Fatalf("present empty value rejected: %v", err)
	}
	if err := Require([]string{}, "REQUIRED"); err == nil {
		t.Fatal("missing value accepted")
	}
	if err := Require([]string{"A=1", "A=2"}, "A"); err == nil {
		t.Fatal("duplicate value accepted")
	}
}
