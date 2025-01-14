package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"whatsapp/internal/domain"

	"github.com/gorilla/websocket"
)

type WebSocketServer struct {
	whatsapp domain.Client
	upgrader websocket.Upgrader
	clients  sync.Map
}

type Message struct {
	Type      string `json:"type"`
	Code      string `json:"code,omitempty"`
	Recipient string `json:"recipient,omitempty"`
	Content   string `json:"content,omitempty"`
	Sender    string `json:"sender,omitempty"`
	MsgType   string `json:"message_type,omitempty"`
	Success   bool   `json:"success,omitempty"`
}

func NewWebSocketServer(whatsapp domain.Client) *WebSocketServer {
	return &WebSocketServer{
		whatsapp: whatsapp,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins in development
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
}

func (s *WebSocketServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection: %v", err)
		return
	}

	// Configure WebSocket connection
	conn.SetReadLimit(512 * 1024) // 512KB
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Create a context that will be canceled when the connection closes
	ctx, cancel := context.WithCancel(r.Context())
	defer func() {
		cancel()
		conn.Close()
	}()

	// Start ping handler
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
					log.Printf("Failed to write ping: %v", err)
					return
				}
			}
		}
	}()

	// Get QR channel
	qrChan, err := s.whatsapp.GetQRChannel(ctx)
	if err != nil {
		log.Printf("Failed to get QR channel: %v", err)
		conn.WriteJSON(Message{
			Type:    "error",
			Content: fmt.Sprintf("Failed to get QR channel: %v", err),
		})
		return
	}

	// Store connection
	connID := fmt.Sprintf("%p", conn)
	s.clients.Store(connID, conn)
	defer s.clients.Delete(connID)

	// Set message handler
	s.whatsapp.SetMessageHandler(func(msg *domain.Message) {
		response := Message{
			Type:    "message",
			Sender:  msg.Sender,
			Content: msg.Content,
			MsgType: msg.Type,
		}
		if err := conn.WriteJSON(response); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("Failed to write message: %v", err)
			}
		}
	})

	// Connect to WhatsApp
	go func() {
		log.Println("Starting WhatsApp connection...")
		if err := s.whatsapp.Connect(ctx); err != nil {
			if ctx.Err() == nil { // Only log if context wasn't canceled
				log.Printf("Failed to connect WhatsApp: %v", err)
				conn.WriteJSON(Message{
					Type:    "error",
					Content: fmt.Sprintf("Failed to connect: %v", err),
				})
			}
			return
		}
		log.Println("WhatsApp connection established")

		// Send connected message
		conn.WriteJSON(Message{Type: "connected"})
	}()

	// Handle QR codes
	go func() {
		log.Println("Starting QR code handler...")
		for {
			select {
			case <-ctx.Done():
				log.Println("QR code handler stopped: context canceled")
				return
			case code, ok := <-qrChan:
				if !ok {
					log.Println("QR channel closed")
					return
				}
				log.Println("Received QR code, sending to client...")
				log.Printf("QR code length: %d", len(code))
				log.Printf("First 50 chars of QR code: %s", code[:min(50, len(code))])
				response := Message{
					Type: "qr",
					Code: code,
				}
				if err := conn.WriteJSON(response); err != nil {
					if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
						log.Printf("Failed to write QR code: %v", err)
					}
					return
				}
				log.Println("QR code sent to client successfully")
			}
		}
	}()

	// Handle incoming messages
	for {
		var msg Message
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		switch msg.Type {
		case "send_message":
			if msg.Recipient == "" || msg.Content == "" {
				log.Printf("Invalid message: recipient or content is empty")
				if err := conn.WriteJSON(Message{
					Type:    "send_response",
					Content: "Recipient and content are required",
				}); err != nil {
					log.Printf("Failed to write error response: %v", err)
				}
				continue
			}

			log.Printf("Sending message to %s: %s", msg.Recipient, msg.Content)
			if err := s.whatsapp.SendMessage(msg.Recipient, msg.Content); err != nil {
				log.Printf("Failed to send message: %v", err)
				if err := conn.WriteJSON(Message{
					Type:    "send_response",
					Success: false,
					Content: fmt.Sprintf("Failed to send message: %v", err),
				}); err != nil {
					log.Printf("Failed to write error response: %v", err)
				}
			} else {
				log.Printf("Message sent successfully to %s", msg.Recipient)
				if err := conn.WriteJSON(Message{
					Type:    "send_response",
					Success: true,
					Content: "Message sent successfully",
				}); err != nil {
					log.Printf("Failed to write success response: %v", err)
				}
			}
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
