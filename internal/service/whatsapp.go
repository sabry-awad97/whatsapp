package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"whatsapp/internal/domain"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

// WhatsAppService implements the domain.Client interface
type WhatsAppService struct {
	client     *whatsmeow.Client
	store      *sqlstore.Container
	container  *sqlstore.Container
	logger     waLog.Logger
	handler    func(*domain.Message)
	mu         sync.RWMutex
	connected  bool
	qrChannel  <-chan whatsmeow.QRChannelItem
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewWhatsAppService creates a new WhatsApp service instance
func NewWhatsAppService(config domain.ClientConfig) (*WhatsAppService, error) {
	dbLog := waLog.Stdout("Database", config.LogLevel, true)
	container, err := sqlstore.New("sqlite", config.DBPath, dbLog)
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %w", err)
	}

	device, err := container.GetFirstDevice()
	if err != nil {
		return nil, fmt.Errorf("failed to get device: %w", err)
	}

	clientLog := waLog.Stdout("Client", config.LogLevel, true)
	client := whatsmeow.NewClient(device, clientLog)
	ctx, cancel := context.WithCancel(context.Background())

	service := &WhatsAppService{
		client:     client,
		store:      container,
		container:  container,
		logger:     clientLog,
		ctx:        ctx,
		cancelFunc: cancel,
	}

	client.AddEventHandler(service.eventHandler)
	return service, nil
}

// Connect implements domain.Client
func (s *WhatsAppService) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return nil
	}

	if s.client.Store.ID == nil {
		qrChan, err := s.client.GetQRChannel(ctx)
		if err != nil {
			return fmt.Errorf("failed to get QR channel: %w", err)
		}
		s.qrChannel = qrChan

		err = s.client.Connect()
		if err != nil {
			return fmt.Errorf("failed to connect: %w", err)
		}

		// Don't wait for connection here, let the QR code handling do that
		return nil
	}

	err := s.client.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// For existing sessions, wait for connection
	for i := 0; i < 10; i++ {
		if s.client.IsConnected() {
			s.connected = true
			return nil
		}
		time.Sleep(time.Second)
	}

	return fmt.Errorf("failed to establish connection")
}

// IsConnected implements domain.Client
func (s *WhatsAppService) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.client == nil {
		return false
	}

	isConnected := s.client.IsConnected()
	hasSession := s.client.Store.ID != nil
	return isConnected && hasSession
}

// GetQR implements domain.Client
func (s *WhatsAppService) GetQR() (string, error) {
	s.mu.RLock()
	if s.qrChannel == nil {
		s.mu.RUnlock()
		return "", fmt.Errorf("QR channel not initialized")
	}
	s.mu.RUnlock()

	// Wait for QR code
	select {
	case evt := <-s.qrChannel:
		if evt.Event == "code" {
			// Start monitoring connection in background
			go s.monitorConnection()
			return evt.Code, nil
		}
		return "", fmt.Errorf("unexpected QR event: %s", evt.Event)
	case <-s.ctx.Done():
		return "", s.ctx.Err()
	case <-time.After(time.Second * 30):
		return "", fmt.Errorf("timeout waiting for QR code")
	}
}

// monitorConnection watches for successful connection after QR scan
func (s *WhatsAppService) monitorConnection() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	timeout := time.After(time.Minute * 2)

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.client.IsConnected() && s.client.Store.ID != nil {
				s.connected = true
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
		case <-timeout:
			return
		case <-s.ctx.Done():
			return
		}
	}
}

// SendMessage implements domain.Client
func (s *WhatsAppService) SendMessage(recipient string, content string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.IsConnected() {
		return fmt.Errorf("client is not connected")
	}

	// Validate phone number format (should be in international format without + or spaces)
	recipient = strings.ReplaceAll(recipient, " ", "")
	recipient = strings.TrimPrefix(recipient, "+")

	// Create recipient JID (Jabber ID)
	recipientJID := types.JID{
		User:   recipient,
		Server: "s.whatsapp.net",
	}

	// Create the message
	msg := &waProto.Message{
		Conversation: proto.String(content),
	}

	// Send the message
	_, err := s.client.SendMessage(context.Background(), recipientJID, msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (s *WhatsAppService) eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.Message:
		if s.handler != nil {
			message := &domain.Message{
				ID:        v.Info.ID,
				Content:   v.Message.GetConversation(),
				Type:      domain.TextMessage,
				Sender:    v.Info.Sender.User,
				Timestamp: v.Info.Timestamp,
			}
			s.handler(message)
		}
	case *events.Connected:
		s.mu.Lock()
		s.connected = true
		s.mu.Unlock()
	case *events.Disconnected:
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
	}
}

// SetMessageHandler sets the handler for incoming messages
func (s *WhatsAppService) SetMessageHandler(handler func(*domain.Message)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = handler
}

// Disconnect implements domain.Client
func (s *WhatsAppService) Disconnect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancelFunc()
	s.client.Disconnect()
	s.connected = false
	return nil
}
