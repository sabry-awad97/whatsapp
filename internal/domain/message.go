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

// MessageHandler defines the interface for handling WhatsApp messages
type MessageHandler interface {
	// HandleMessage processes an incoming WhatsApp message
	HandleMessage(message *Message) error
}

// MessageReceiver defines the interface for receiving WhatsApp messages
type MessageReceiver interface {
	// Receive processes an incoming WhatsApp message
	Receive(message *Message) error
	// HandleIncoming sets a handler for incoming messages
	HandleIncoming(handler func(*Message))
}
