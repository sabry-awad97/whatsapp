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

// Constants for WebSocket configuration
const (
	maxMessageSize = 512 * 1024 // 512KB
	pingInterval   = 30 * time.Second
	pongWait       = 60 * time.Second
	writeWait      = 10 * time.Second
)

// Message represents a structured message for WebSocket communication.
// It includes various fields to handle different types of messages and their content.
type Message struct {
	Type      string `json:"type"`                // Type of the message
	Code      string `json:"code,omitempty"`      // QR code or other coded information
	Success   bool   `json:"success,omitempty"`   // Indicates if an operation was successful
	Content   string `json:"content,omitempty"`   // Main content of the message
	Recipient string `json:"recipient,omitempty"` // Recipient of the message
	Sender    string `json:"sender,omitempty"`    // Sender of the message
	MsgType   string `json:"msg_type,omitempty"`  // Specific type of the message content
	FileData  string `json:"file_data,omitempty"` // Base64 encoded file data
	FileName  string `json:"file_name,omitempty"` // Name of the file
	FileType  string `json:"file_type,omitempty"` // MIME type of the file
}

// WebSocketServer handles WebSocket connections and manages communication
// between the WhatsApp client and connected WebSocket clients.
type WebSocketServer struct {
	whatsapp domain.Client
	upgrader websocket.Upgrader
	clients  sync.Map
	// Channel for broadcasting messages
	broadcast chan []byte
}

// NewWebSocketServer creates and returns a new WebSocketServer instance.
// It initializes the server with the provided WhatsApp client and configures
// the WebSocket upgrader to allow all origins.
func NewWebSocketServer(whatsapp domain.Client) *WebSocketServer {
	return &WebSocketServer{
		whatsapp: whatsapp,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for now
			},
		},
		clients:   sync.Map{},
		broadcast: make(chan []byte),
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
	if err := s.configureConnection(conn); err != nil {
		log.Printf("Failed to configure connection: %v", err)
		return
	}

	// Create a context that will be canceled when the connection closes
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Start ping handler
	go s.handlePing(ctx, conn, cancel)

	// Store connection
	connID := fmt.Sprintf("%p", conn)
	s.clients.Store(connID, conn)
	defer s.clients.Delete(connID)

	// Create error channel for async operations
	errChan := make(chan error, 1)
	defer close(errChan)

	// Set message handler
	s.setupMessageHandler(conn, errChan)

	// Handle authentication and connection state
	if err := s.handleConnectionState(ctx, conn); err != nil {
		log.Printf("Connection state error: %v", err)
		return
	}

	// Handle incoming messages
	s.handleClientMessages(ctx, conn, errChan)
}

func (s *WebSocketServer) configureConnection(conn *websocket.Conn) error {
	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	return nil
}

func (s *WebSocketServer) handlePing(ctx context.Context, conn *websocket.Conn, cancel context.CancelFunc) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait)); err != nil {
				log.Printf("Failed to write ping: %v", err)
				cancel()
				return
			}
		}
	}
}

func (s *WebSocketServer) setupMessageHandler(conn *websocket.Conn, errChan chan error) {
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
}

func (s *WebSocketServer) handleConnectionState(ctx context.Context, conn *websocket.Conn) error {
	if s.whatsapp.IsLoggedIn() {
		return s.handleLoggedInState(conn)
	}
	return s.handleQRCodeFlow(ctx, conn)
}

func (s *WebSocketServer) handleLoggedInState(conn *websocket.Conn) error {
	if !s.whatsapp.IsConnected() {
		log.Println("Client logged in but not connected, waiting for connection...")
		time.Sleep(2 * time.Second)
	}

	if s.whatsapp.IsConnected() {
		log.Println("Already logged in and connected, sending connected message")
		return conn.WriteJSON(Message{Type: "connected"})
	}

	log.Println("Not connected, proceeding with QR code flow")
	return nil
}

func (s *WebSocketServer) handleQRCodeFlow(ctx context.Context, conn *websocket.Conn) error {
	qrChan, err := s.whatsapp.GetQRChannel(ctx)
	if err != nil {
		return fmt.Errorf("failed to get QR channel: %v", err)
	}

	if qrChan == nil {
		log.Println("No QR code needed, sending connected message")
		return conn.WriteJSON(Message{Type: "connected"})
	}

	log.Println("Starting QR code handler...")
	return s.processQRCodes(ctx, conn, qrChan)
}

func (s *WebSocketServer) processQRCodes(ctx context.Context, conn *websocket.Conn, qrChan chan string) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("QR code handler stopped: context canceled")
			return nil
		case code, ok := <-qrChan:
			if !ok {
				log.Println("QR channel closed")
				if s.whatsapp.IsLoggedIn() && s.whatsapp.IsConnected() {
					log.Println("Logged in and connected successfully, sending connected message")
					return conn.WriteJSON(Message{Type: "connected"})
				}
				return nil
			}
			if err := s.sendQRCode(conn, code); err != nil {
				return err
			}
		}
	}
}

func (s *WebSocketServer) sendQRCode(conn *websocket.Conn, code string) error {
	log.Printf("Received QR code, length: %d", len(code))
	response := Message{
		Type: "qr",
		Code: code,
	}
	if err := conn.WriteJSON(response); err != nil {
		if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
			return fmt.Errorf("failed to write QR code: %v", err)
		}
	}
	log.Println("QR code sent to client successfully")
	return nil
}

func (s *WebSocketServer) handleClientMessages(_ context.Context, conn *websocket.Conn, errChan chan error) {
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

			conn.SetReadDeadline(time.Now().Add(pongWait))

			switch msg.Type {
			case "send_message":
				if err := s.handleTextMessage(conn, &msg); err != nil {
					return
				}
			case "send_file":
				if err := s.handleFileMessage(conn, &msg); err != nil {
					return
				}
			}
		}
	}
}

func (s *WebSocketServer) handleTextMessage(conn *websocket.Conn, msg *Message) error {
	if msg.Recipient == "" || msg.Content == "" {
		return s.sendErrorResponse(conn, "Recipient and content are required")
	}

	log.Printf("Sending message to %s: %s", msg.Recipient, msg.Content)
	if err := s.whatsapp.SendMessage(msg.Recipient, msg.Content); err != nil {
		return s.sendErrorResponse(conn, fmt.Sprintf("Failed to send message: %v", err))
	}

	if err := s.sendSuccessResponse(conn, "Message sent successfully"); err != nil {
		return err
	}

	log.Printf("Message sent successfully to %s", msg.Recipient)
	return nil
}

func (s *WebSocketServer) handleFileMessage(conn *websocket.Conn, msg *Message) error {
	if msg.Recipient == "" || msg.FileData == "" {
		return s.sendErrorResponse(conn, "Recipient and file data are required")
	}

	fileData, err := base64.StdEncoding.DecodeString(msg.FileData)
	if err != nil {
		return s.sendErrorResponse(conn, fmt.Sprintf("Failed to decode file data: %v", err))
	}

	tmpFile, err := s.createTempFile(fileData)
	if err != nil {
		return s.sendErrorResponse(conn, err.Error())
	}
	defer os.Remove(tmpFile.Name())

	log.Printf("Sending file to %s: %s", msg.Recipient, msg.FileName)
	if err := s.whatsapp.SendFile(msg.Recipient, tmpFile.Name(), msg.FileName, msg.FileType); err != nil {
		return s.sendErrorResponse(conn, fmt.Sprintf("Failed to send file: %v", err))
	}

	if err := s.sendSuccessResponse(conn, "File sent successfully"); err != nil {
		return err
	}

	log.Printf("File sent successfully to %s", msg.Recipient)
	return nil
}

func (s *WebSocketServer) createTempFile(fileData []byte) (*os.File, error) {
	tmpFile, err := os.CreateTemp("", "whatsapp-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %v", err)
	}

	if _, err := tmpFile.Write(fileData); err != nil {
		os.Remove(tmpFile.Name())
		tmpFile.Close()
		return nil, fmt.Errorf("failed to write file data: %v", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("failed to close temporary file: %v", err)
	}

	return tmpFile, nil
}

func (s *WebSocketServer) sendErrorResponse(conn *websocket.Conn, message string) error {
	log.Printf("Error: %s", message)
	return conn.WriteJSON(Message{
		Type:    "send_response",
		Success: false,
		Content: message,
	})
}

func (s *WebSocketServer) sendSuccessResponse(conn *websocket.Conn, message string) error {
	return conn.WriteJSON(Message{
		Type:    "send_response",
		Success: true,
		Content: message,
	})
}
