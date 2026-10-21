package server

import "net/http"

func (s *Server) handleCloudflareTokenVerify(w http.ResponseWriter, r *http.Request) {
	cloudflareResult(w, map[string]any{"id": "cellhive", "status": "active"})
}

func (s *Server) handleCloudflareUser(w http.ResponseWriter, r *http.Request) {
	cloudflareResult(w, map[string]any{"id": "cellhive", "email": "cellhive@localhost"})
}
