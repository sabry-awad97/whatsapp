package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"whatsapp/internal/domain"
)

type Message struct {
	Type      string `json:"type"`
	Code      string `json:"code,omitempty"`
	Success   bool   `json:"success,omitempty"`
	Content   string `json:"content,omitempty"`
	Recipient string `json:"recipient,omitempty"`
	Sender    string `json:"sender,omitempty"`
	MsgType   string `json:"msg_type,omitempty"`
	FileData  string `json:"file_data,omitempty"`
	FileName  string `json:"file_name,omitempty"`
	FileType  string `json:"file_type,omitempty"`
}

type WebSocketServer struct {
	whatsapp domain.Client
	upgrader websocket.Upgrader
	clients  sync.Map
}

func NewWebSocketServer(whatsapp domain.Client) *WebSocketServer {
	return &WebSocketServer{
		whatsapp: whatsapp,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for now
			},
		},
	}
}

func (s *WebSocketServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	log.Println("New WebSocket connection request")
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection: %v", err)
		return
	}
	defer conn.Close()

	// Configure WebSocket connection
	conn.SetReadLimit(512 * 1024) // 512KB
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Create a context that will be canceled when the connection closes
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

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
					cancel()
					return
				}
			}
		}
	}()

	// Store connection
	connID := fmt.Sprintf("%p", conn)
	s.clients.Store(connID, conn)
	defer s.clients.Delete(connID)

	// Create error channel for async operations
	errChan := make(chan error, 1)
	defer close(errChan)

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
				errChan <- err
			}
		}
	})

	// Handle initial connection state
	if s.whatsapp.IsLoggedIn() {
		if !s.whatsapp.IsConnected() {
			log.Println("Client logged in but not connected, waiting for connection...")
			time.Sleep(2 * time.Second) // Give some time for the connection to establish
		}
		if s.whatsapp.IsConnected() {
			log.Println("Already logged in and connected, sending connected message")
			if err := conn.WriteJSON(Message{Type: "connected"}); err != nil {
				log.Printf("Failed to write connected message: %v", err)
				return
			}
		} else {
			log.Println("Not connected, proceeding with QR code flow")
		}
	}

	// Get QR channel for new login if needed
	if !s.whatsapp.IsLoggedIn() || !s.whatsapp.IsConnected() {
		log.Println("Getting QR channel for new login")
		qrChan, err := s.whatsapp.GetQRChannel(ctx)
		if err != nil {
			log.Printf("Failed to get QR channel: %v", err)
			conn.WriteJSON(Message{
				Type:    "error",
				Content: fmt.Sprintf("Failed to get QR channel: %v", err),
			})
			return
		}

		// Handle QR codes if needed
		if qrChan != nil {
			log.Println("Starting QR code handler...")
			for {
				select {
				case <-ctx.Done():
					log.Println("QR code handler stopped: context canceled")
					return
				case code, ok := <-qrChan:
					if !ok {
						log.Println("QR channel closed")
						if s.whatsapp.IsLoggedIn() && s.whatsapp.IsConnected() {
							log.Println("Logged in and connected successfully, sending connected message")
							if err := conn.WriteJSON(Message{Type: "connected"}); err != nil {
								log.Printf("Failed to write connected message: %v", err)
							}
						}
						return
					}
					log.Printf("Received QR code, length: %d", len(code))
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
		} else {
			log.Println("No QR code needed, sending connected message")
			if err := conn.WriteJSON(Message{Type: "connected"}); err != nil {
				log.Printf("Failed to write connected message: %v", err)
				return
			}
		}
	}

	// Handle incoming messages
	for {
		select {
		case err := <-errChan:
			log.Printf("Async error occurred: %v", err)
			return
		default:
			var msg Message
			if err := conn.ReadJSON(&msg); err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("WebSocket error: %v", err)
				}
				return
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
						return
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
						return
					}
					continue
				}

				if err := conn.WriteJSON(Message{
					Type:    "send_response",
					Success: true,
					Content: "Message sent successfully",
				}); err != nil {
					log.Printf("Failed to write success response: %v", err)
					return
				}
				log.Printf("Message sent successfully to %s", msg.Recipient)

			case "send_file":
				// Handle file sending
				if msg.Recipient == "" || msg.FileData == "" {
					log.Printf("Invalid file message: recipient or file data is empty")
					if err := conn.WriteJSON(Message{
						Type:    "send_response",
						Content: "Recipient and file data are required",
					}); err != nil {
						log.Printf("Failed to write error response: %v", err)
						return
					}
					continue
				}

				// Decode base64 file data
				fileData, err := base64.StdEncoding.DecodeString(msg.FileData)
				if err != nil {
					log.Printf("Failed to decode file data: %v", err)
					if err := conn.WriteJSON(Message{
						Type:    "send_response",
						Success: false,
						Content: fmt.Sprintf("Failed to decode file data: %v", err),
					}); err != nil {
						log.Printf("Failed to write error response: %v", err)
						return
					}
					continue
				}

				// Create temporary file
				tmpFile, err := os.CreateTemp("", "whatsapp-*")
				if err != nil {
					log.Printf("Failed to create temporary file: %v", err)
					if err := conn.WriteJSON(Message{
						Type:    "send_response",
						Success: false,
						Content: fmt.Sprintf("Failed to create temporary file: %v", err),
					}); err != nil {
						log.Printf("Failed to write error response: %v", err)
						return
					}
					continue
				}
				defer os.Remove(tmpFile.Name())

				// Write file data
				if _, err := tmpFile.Write(fileData); err != nil {
					log.Printf("Failed to write file data: %v", err)
					if err := conn.WriteJSON(Message{
						Type:    "send_response",
						Success: false,
						Content: fmt.Sprintf("Failed to write file data: %v", err),
					}); err != nil {
						log.Printf("Failed to write error response: %v", err)
						return
					}
					continue
				}
				tmpFile.Close()

				log.Printf("Sending file to %s: %s", msg.Recipient, msg.FileName)
				if err := s.whatsapp.SendFile(msg.Recipient, tmpFile.Name(), msg.FileName, msg.FileType); err != nil {
					log.Printf("Failed to send file: %v", err)
					if err := conn.WriteJSON(Message{
						Type:    "send_response",
						Success: false,
						Content: fmt.Sprintf("Failed to send file: %v", err),
					}); err != nil {
						log.Printf("Failed to write error response: %v", err)
						return
					}
					continue
				}

				if err := conn.WriteJSON(Message{
					Type:    "send_response",
					Success: true,
					Content: "File sent successfully",
				}); err != nil {
					log.Printf("Failed to write success response: %v", err)
					return
				}
				log.Printf("File sent successfully to %s", msg.Recipient)
			}
		}
	}
}
