package server

import (
	"log"
	"net/http"
	"path/filepath"
)

type HTTPServer struct {
	wsServer *WebSocketServer
	mux      *http.ServeMux
}

func NewHTTPServer(wsServer *WebSocketServer) *HTTPServer {
	mux := http.NewServeMux()

	// Serve static files from the web directory
	webDir := "./web"
	fs := http.FileServer(http.Dir(webDir))
	
	// Handle root path
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.ServeFile(w, r, filepath.Join(webDir, "index.html"))
			return
		}
		fs.ServeHTTP(w, r)
	})

	mux.HandleFunc("/ws", wsServer.HandleWebSocket)

	return &HTTPServer{
		wsServer: wsServer,
		mux:      mux,
	}
}

func (s *HTTPServer) Start(addr string) error {
	log.Printf("Starting server on %s", addr)
	return http.ListenAndServe(addr, s.mux)
}
