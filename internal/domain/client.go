package domain

import "context"

// Client represents a WhatsApp client
type Client interface {
	Connect(ctx context.Context) error
	Disconnect() error
	IsConnected() bool
	GetQR() (string, error)
	SendMessage(recipient string, content string) error
}

// ClientConfig holds the configuration for WhatsApp client
type ClientConfig struct {
	DBPath   string
	LogLevel string
}
