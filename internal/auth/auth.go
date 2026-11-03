// Package auth defines authenticated control-plane principals. JWT verification
// lives in bearertoken; authoritative issuer policy is enforced by the server.
package auth

import "cellhive/internal/bearertoken"

// Principal carries the authenticated identity and business namespace grants.
// All does not grant access to reserved platform namespaces; those require an
// explicit bootstrap/platform authorization check at the endpoint.
type Principal struct {
	Sub        string   `json:"sub"`
	Kind       string   `json:"kind"` // admin|namespace|binding|internal
	All        bool     `json:"all"`
	NS         []string `json:"ns,omitempty"`
	OnBehalfOf string   `json:"on_behalf_of,omitempty"`
	RequestID  string   `json:"request_id,omitempty"`
}

// Allows reports whether the principal may manage an exact business namespace.
func (p Principal) Allows(ns string) bool {
	if p.All {
		return bearertoken.Allows([]string{"*"}, ns)
	}
	return bearertoken.Allows(p.NS, ns)
}
