package domain

import "time"

// Message represents a WhatsApp message
type Message struct {
	ID        string
	Content   string
	Type      MessageType
	Sender    string
	Recipient string
	Timestamp time.Time
}

// MessageType represents the type of message
type MessageType string

const (
	TextMessage     MessageType = "text"
	ImageMessage    MessageType = "image"
	DocumentMessage MessageType = "document"
)

// MessageRepository defines the interface for message persistence
type MessageRepository interface {
	Save(message *Message) error
	FindByID(id string) (*Message, error)
	FindBySender(sender string) ([]*Message, error)
}

// MessageService defines the interface for message handling
type MessageService interface {
	Send(message *Message) error
	Receive(message *Message) error
	HandleIncoming(handler func(*Message))
}
