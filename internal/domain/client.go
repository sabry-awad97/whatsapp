package domain

import "context"

// Client represents a WhatsApp client
type Client interface {
	// Connect establishes a connection to WhatsApp
	Connect(ctx context.Context) error

	// Disconnect closes the WhatsApp connection
	Disconnect() error

	// IsConnected returns true if connected to WhatsApp
	IsConnected() bool

	// IsLoggedIn returns true if logged in to WhatsApp
	IsLoggedIn() bool

	// SendMessage sends a message to the specified recipient
	SendMessage(recipient string, content string) error

	// SendFile sends a file to the specified recipient
	SendFile(recipient string, filePath string, fileName string, fileType string) error

	// GetQRChannel returns a channel that will receive QR codes for WhatsApp Web
	GetQRChannel(ctx context.Context) (chan string, error)

	// SetMessageHandler sets the handler for incoming messages
	SetMessageHandler(handler func(*Message))
}

// ClientConfig holds the configuration for WhatsApp client
type ClientConfig struct {
	DBPath   string
	LogLevel string
}

// Message represents a WhatsApp message
type Message struct {
	Sender  string `json:"sender"`
	Content string `json:"content"`
	Type    string `json:"type"`
}
