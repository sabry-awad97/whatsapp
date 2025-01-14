package service

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"whatsapp/internal/domain"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type WhatsAppService struct {
	store    *sqlstore.Container
	clientMu sync.Mutex
	client   *whatsmeow.Client
	qrChan   chan string
	handler  func(*domain.Message)
}

func NewWhatsAppService(container *sqlstore.Container) (*WhatsAppService, error) {
	if container == nil {
		return nil, fmt.Errorf("container is required")
	}

	return &WhatsAppService{
		store:  container,
		qrChan: make(chan string),
	}, nil
}

func (s *WhatsAppService) initClient() error {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	log.Println("Initializing WhatsApp client...")

	if s.client != nil {
		log.Println("Disconnecting existing client...")
		s.client.Disconnect()
		s.client = nil
	}

	deviceStore, err := s.store.GetFirstDevice()
	if err != nil {
		log.Printf("Failed to get device: %v", err)
		return fmt.Errorf("failed to get device: %v", err)
	}

	client := whatsmeow.NewClient(deviceStore, nil)
	client.AddEventHandler(s.handleEvent)
	s.client = client

	log.Println("WhatsApp client initialized successfully")
	return nil
}

func (s *WhatsAppService) handleEvent(evt interface{}) {
	switch v := evt.(type) {
	case *events.QR:
		log.Printf("Received QR code event with %d codes", len(v.Codes))
		if len(v.Codes) > 0 {
			qrCode := v.Codes[0]
			log.Printf("QR code data length: %d", len(qrCode))
			log.Printf("First 50 chars of QR code: %s", qrCode[:min(50, len(qrCode))])
			select {
			case s.qrChan <- qrCode:
				log.Println("Successfully sent QR code to channel")
			default:
				log.Println("QR channel is full, could not send QR code")
			}
		} else {
			log.Println("No QR codes received in event")
		}

	case *events.Connected:
		log.Println("WhatsApp client connected successfully")

	case *events.LoggedOut:
		log.Println("WhatsApp client logged out")

	case *events.Message:
		if s.handler != nil {
			log.Printf("Received message from %s", v.Info.Sender.String())

			var content string
			var msgType string

			switch {
			case v.Message.GetConversation() != "":
				content = v.Message.GetConversation()
				msgType = "text"

			case v.Message.ExtendedTextMessage != nil:
				content = v.Message.ExtendedTextMessage.GetText()
				msgType = "text"

			case v.Message.ImageMessage != nil:
				content = "[Image]"
				if v.Message.ImageMessage.Caption != nil {
					content += " " + *v.Message.ImageMessage.Caption
				}
				msgType = "image"

			case v.Message.VideoMessage != nil:
				content = "[Video]"
				if v.Message.VideoMessage.Caption != nil {
					content += " " + *v.Message.VideoMessage.Caption
				}
				msgType = "video"

			case v.Message.DocumentMessage != nil:
				content = "[Document]"
				if v.Message.DocumentMessage.Title != nil {
					content += " " + *v.Message.DocumentMessage.Title
				}
				msgType = "document"

			case v.Message.AudioMessage != nil:
				content = "[Audio]"
				msgType = "audio"

			case v.Message.StickerMessage != nil:
				content = "[Sticker]"
				msgType = "sticker"

			default:
				content = "[Unsupported message type]"
				msgType = "unknown"
			}

			log.Printf("Message type: %s, content: %s", msgType, content)
			if content != "" {
				s.handler(&domain.Message{
					Sender:  v.Info.Sender.String(),
					Content: content,
					Type:    msgType,
				})
			}
		}

	case *events.Receipt:
		log.Printf("Message receipt from %s: %+v", v.Sender, v.Type)

	case *events.Presence:
		log.Printf("Presence update from %s: %v", v.From, v.Unavailable)

	case *events.HistorySync:
		log.Printf("Received history sync: %d messages", len(v.Data.GetConversations()))

	case *events.AppState:
		log.Printf("App state update: %s", v.Index)

	case *events.PushName:
		log.Printf("Push name update for %s: %s", v.JID, v.NewPushName)

	case *events.ClientOutdated:
		log.Println("WhatsApp client is outdated")

	case *events.StreamReplaced:
		log.Println("Stream replaced")

	case *events.OfflineSyncCompleted:
		log.Printf("Offline sync completed: %d messages processed", v.Count)

	default:
		// Only log unhandled event types that we haven't explicitly handled above
		log.Printf("Unhandled event type: %T", v)
	}
}

func (s *WhatsAppService) GetQRChannel(ctx context.Context) (chan string, error) {
	log.Println("Getting QR channel...")
	if err := s.initClient(); err != nil {
		return nil, err
	}
	log.Println("Returning QR channel")
	return s.qrChan, nil
}

func (s *WhatsAppService) Connect(ctx context.Context) error {
	s.clientMu.Lock()
	if s.client == nil {
		s.clientMu.Unlock()
		return fmt.Errorf("client not initialized")
	}
	client := s.client
	s.clientMu.Unlock()

	log.Println("Connecting WhatsApp client...")
	err := client.Connect()
	if err != nil {
		log.Printf("Failed to connect: %v", err)
		return fmt.Errorf("failed to connect: %v", err)
	}
	log.Println("WhatsApp client connected")
	return nil
}

func (s *WhatsAppService) IsConnected() bool {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	return s.client != nil && s.client.IsConnected()
}

func (s *WhatsAppService) SendMessage(recipient string, content string) error {
	s.clientMu.Lock()
	if s.client == nil {
		s.clientMu.Unlock()
		return fmt.Errorf("client not initialized")
	}
	client := s.client
	s.clientMu.Unlock()

	recipient = strings.TrimSpace(recipient)
	if !strings.HasSuffix(recipient, "@s.whatsapp.net") {
		recipient = recipient + "@s.whatsapp.net"
	}

	jid, err := types.ParseJID(recipient)
	if err != nil {
		return fmt.Errorf("invalid phone number: %v", err)
	}

	msg := &waProto.Message{
		Conversation: proto.String(content),
	}

	// Add retry logic for sending messages
	var lastErr error
	for i := 0; i < 3; i++ {
		_, err = client.SendMessage(context.Background(), jid, msg)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("Attempt %d: Failed to send message: %v", i+1, err)
		time.Sleep(time.Second * time.Duration(i+1))
	}

	return fmt.Errorf("failed to send message after 3 attempts: %v", lastErr)
}

func (s *WhatsAppService) SetMessageHandler(handler func(*domain.Message)) {
	s.handler = handler
}

func (s *WhatsAppService) Disconnect() {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.client != nil {
		s.client.Disconnect()
		s.client = nil
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
