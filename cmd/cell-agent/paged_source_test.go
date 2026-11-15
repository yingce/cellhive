package main

import (
	"reflect"
	"testing"

	"cellhive/internal/pagedvfs"
)

func TestPagedSourceRequiresStablePointerIdentity(t *testing.T) {
	source := reflect.TypeOf((*pagedvfs.Source)(nil)).Elem()
	closer := reflect.TypeOf((*interface{ Close() error })(nil)).Elem()
	if reflect.TypeOf(pagedSource{}).Implements(source) {
		t.Fatal("value adapter can reach production activation without a stable Close identity")
	}
	ptr := reflect.TypeOf((*pagedSource)(nil))
	if !ptr.Implements(source) || !ptr.Implements(closer) {
		t.Fatal("pointer adapter must retain paging and Close capabilities")
	}
}
