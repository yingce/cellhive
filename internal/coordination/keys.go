package coordination

import (
	"encoding/json"
	"errors"
	"strings"
)

const BindingKey = "fleet/coordination.json"

// Selected is deliberately smaller than the bucket provider's authority set:
// recovery logs and R2 metadata remain durable in the data store.
func Selected(k string) bool {
	return strings.HasPrefix(k, "nodes/") || strings.HasPrefix(k, "wake/") || k == "fleet/drain-token.json" || k == "fleet/waker.json" || (strings.HasPrefix(k, "cells/") && (strings.HasSuffix(k, "/own.json") || strings.HasSuffix(k, "/owner-gen")))
}
func purePrefix(p string) bool {
	return strings.HasPrefix(p, "nodes/") || strings.HasPrefix(p, "wake/") || Selected(p)
}
func mayContain(p string) bool {
	return purePrefix(p) || p == "" || strings.HasPrefix("cells/", p) || strings.HasPrefix(p, "cells/") && !strings.Contains(p, "/ltx/") || strings.HasPrefix("fleet/", p) || p == "fleet/"
}
func ownerMember(k string, v []byte) (string, error) {
	if len(v) == 0 || !strings.HasPrefix(k, "cells/") || !strings.HasSuffix(k, "/own.json") {
		return "", nil
	}
	var o struct {
		Node, Session string
		Retired       bool
	}
	if err := json.Unmarshal(v, &o); err != nil {
		return "", nil // Generic conformance probes may use non-JSON values.
	}
	if strings.ContainsRune(o.Node, '\x00') || strings.ContainsRune(o.Session, '\x00') {
		return "", errors.New("coordination: invalid owner identity")
	}
	if o.Node == "" || o.Retired {
		return "", nil
	}
	return o.Node + "\x00" + o.Session + "\x00" + k, nil
}
