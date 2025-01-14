package server

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"whatsapp/internal/domain"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

type WebSocketServer struct {
	whatsapp domain.Client
	clients  map[*websocket.Conn]bool
	mu       sync.RWMutex
}

type Message struct {
	Type    string `json:"type"`
	Phone   string `json:"phone"`
	Content string `json:"content"`
}

func NewWebSocketServer(whatsapp domain.Client) *WebSocketServer {
	return &WebSocketServer{
		whatsapp: whatsapp,
		clients:  make(map[*websocket.Conn]bool),
	}
}

func (s *WebSocketServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection: %v", err)
		return
	}
	defer conn.Close()

	s.mu.Lock()
	s.clients[conn] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		var message Message
		if err := json.Unmarshal(msg, &message); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		switch message.Type {
		case "send_message":
			err := s.whatsapp.SendMessage(message.Phone, message.Content)
			response := map[string]interface{}{
				"type": "send_response",
				"success": err == nil,
			}
			if err != nil {
				response["error"] = err.Error()
			}
			
			responseJSON, _ := json.Marshal(response)
			if err := conn.WriteMessage(websocket.TextMessage, responseJSON); err != nil {
				log.Printf("Failed to send response: %v", err)
			}
		}
	}
}

func (s *WebSocketServer) BroadcastIncomingMessage(msg *domain.Message) {
	response := map[string]interface{}{
		"type":    "received_message",
		"from":    msg.Sender,
		"content": msg.Content,
	}
	
	responseJSON, _ := json.Marshal(response)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for client := range s.clients {
		if err := client.WriteMessage(websocket.TextMessage, responseJSON); err != nil {
			log.Printf("Failed to broadcast message: %v", err)
		}
	}
}
