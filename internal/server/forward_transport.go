package server

import (
	"net/http"
	"time"
)

// Forwarding shares a connection pool across requests. The net/http default
// retains only two idle connections per host, which churns connections during
// concurrent KV traffic and can exhaust the entry node's ephemeral ports.
var defaultForwardClient = newForwardClient()

func newForwardClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 128
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}
}

func (s *Server) forwardHTTPClient() *http.Client {
	if s.ForwardClient != nil {
		return s.ForwardClient
	}
	return defaultForwardClient
}
