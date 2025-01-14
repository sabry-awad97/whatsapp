package domain

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
