package server

import (
	"log"
	"net/http"
	"path/filepath"
)

type HTTPServer struct {
	wsServer *WebSocketServer
}

func NewHTTPServer(wsServer *WebSocketServer) *HTTPServer {
	return &HTTPServer{
		wsServer: wsServer,
	}
}

func (s *HTTPServer) Start(addr string) error {
	// Set up routes
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.ServeFile(w, r, filepath.Join("internal", "server", "static", "index.html"))
			return
		}
		http.FileServer(http.Dir(filepath.Join("internal", "server", "static"))).ServeHTTP(w, r)
	})
	http.HandleFunc("/ws", s.wsServer.HandleWebSocket)

	log.Printf("Starting server on %s", addr)
	return http.ListenAndServe(addr, nil)
}
