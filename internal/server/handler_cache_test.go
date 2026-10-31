package server

import "testing"

func TestHandlerMuxIsCached(t *testing.T) {
	s := newScopeServer(t)
	s.Handler()
	s.Handler()
	if got := s.handlerBuilds.Load(); got != 1 {
		t.Fatalf("Handler built the mux %d times, want 1", got)
	}
	s.AdminHandler()
	s.AdminHandler()
	if got := s.adminBuilds.Load(); got != 1 {
		t.Fatalf("AdminHandler built the mux %d times, want 1", got)
	}
}
