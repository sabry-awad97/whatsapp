package domain

import "context"

// Client represents a WhatsApp client
type Client interface {
	// Connect establishes a connection to WhatsApp
	Connect(ctx context.Context) error

	// GetQRChannel returns a channel that will receive the QR code for authentication
	GetQRChannel(ctx context.Context) (chan string, error)

	// IsConnected returns true if connected to WhatsApp
	IsConnected() bool

	// SendMessage sends a message to the specified recipient
	SendMessage(recipient, content string) error

	// Disconnect closes the WhatsApp connection
	Disconnect()

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
	Sender  string
	Content string
}
