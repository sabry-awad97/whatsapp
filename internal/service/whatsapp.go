package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	"whatsapp/internal/domain"
)

type WhatsAppService struct {
	client   *whatsmeow.Client
	clientMu sync.Mutex
	qrChan   chan string
	handler  func(*domain.Message)
	dbConn   *sqlstore.Container
}

func NewWhatsAppService() (*WhatsAppService, error) {
	log.Println("Initializing WhatsApp service...")

	// Initialize WhatsApp database
	dbLog := waLog.Stdout("Database", "DEBUG", true)
	container, err := sqlstore.New("sqlite", "file:whatsapp.db?_foreign_keys=1&_pragma=foreign_keys(1)", dbLog)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}

	return &WhatsAppService{
		dbConn: container,
	}, nil
}

func (s *WhatsAppService) initClient() error {
	log.Println("Initializing WhatsApp client...")

	// Get device store
	deviceStore, err := s.dbConn.GetFirstDevice()
	if err != nil {
		log.Printf("Failed to get device: %v", err)
		// Create new device if none exists
		log.Println("Creating new device...")
		deviceStore = s.dbConn.NewDevice()
	}

	// Initialize client if not already initialized
	if s.client == nil {
		log.Println("Creating new WhatsApp client...")
		clientLog := waLog.Stdout("Client", "DEBUG", true)
		s.client = whatsmeow.NewClient(deviceStore, clientLog)
		s.client.AddEventHandler(s.handleEvent)
		log.Println("WhatsApp client initialized successfully")
	} else {
		log.Println("WhatsApp client already initialized")
	}

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
		// Clear the device store to force new QR code on next connection
		s.clientMu.Lock()
		if s.client != nil {
			if store := s.client.Store; store != nil {
				if err := store.Delete(); err != nil {
					log.Printf("Failed to delete device store: %v", err)
				}
			}
		}
		s.clientMu.Unlock()

	case *events.Message:
		if s.handler != nil {
			log.Printf("Received message from %s", v.Info.Sender.String())

			var content string
			var msgType string

			if v.Message == nil {
				log.Println("Message is nil, skipping")
				return
			}

			// Try to extract message content based on type
			switch {
			case v.Message.Conversation != nil && *v.Message.Conversation != "":
				content = *v.Message.Conversation
				msgType = "text"
				log.Printf("Text message: %s", content)

			case v.Message.ExtendedTextMessage != nil:
				content = *v.Message.ExtendedTextMessage.Text
				msgType = "text"
				log.Printf("Extended text message: %s", content)

			case v.Message.ImageMessage != nil:
				if v.Message.ImageMessage.Caption != nil {
					content = *v.Message.ImageMessage.Caption
				} else {
					content = "[Image]"
				}
				msgType = "image"
				log.Printf("Image message with caption: %s", content)

			case v.Message.VideoMessage != nil:
				if v.Message.VideoMessage.Caption != nil {
					content = *v.Message.VideoMessage.Caption
				} else {
					content = "[Video]"
				}
				msgType = "video"
				log.Printf("Video message with caption: %s", content)

			case v.Message.DocumentMessage != nil:
				if v.Message.DocumentMessage.Title != nil {
					content = *v.Message.DocumentMessage.Title
				} else {
					content = "[Document]"
				}
				msgType = "document"
				log.Printf("Document message with title: %s", content)

			case v.Message.AudioMessage != nil:
				content = "[Audio]"
				msgType = "audio"
				log.Printf("Audio message")

			case v.Message.StickerMessage != nil:
				content = "[Sticker]"
				msgType = "sticker"
				log.Printf("Sticker message")

			default:
				content = "[Unsupported message type]"
				msgType = "unknown"
				log.Printf("Unknown message type: %T", v.Message)
			}

			s.handler(&domain.Message{
				Sender:  v.Info.Sender.String(),
				Content: content,
				Type:    msgType,
			})
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

func (s *WhatsAppService) Connect(ctx context.Context) error {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	// Initialize client if needed
	if err := s.initClient(); err != nil {
		return fmt.Errorf("failed to initialize client: %v", err)
	}

	// Connect to WhatsApp
	log.Println("Connecting to WhatsApp...")
	if err := s.client.Connect(); err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}

	// Wait for context cancellation
	go func() {
		<-ctx.Done()
		log.Println("Context canceled, disconnecting WhatsApp client")
		s.Disconnect()
	}()

	return nil
}

func (s *WhatsAppService) GetQRChannel(ctx context.Context) (chan string, error) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	log.Println("Getting QR channel...")

	// Initialize client if needed
	if err := s.initClient(); err != nil {
		log.Printf("Failed to initialize client: %v", err)
		return nil, fmt.Errorf("failed to initialize client: %v", err)
	}

	// Check if already logged in
	if s.client.Store.ID != nil {
		// Ensure we're connected
		if !s.client.IsConnected() {
			log.Println("Client logged in but not connected, connecting...")
			if err := s.client.Connect(); err != nil {
				log.Printf("Failed to connect: %v", err)
				return nil, fmt.Errorf("failed to connect: %v", err)
			}
		}
		log.Println("Already logged in, no need for QR code")
		return nil, nil
	}

	// Create QR channel if needed
	if s.qrChan == nil {
		log.Println("Creating new QR channel")
		s.qrChan = make(chan string)

		// Get QR channel from WhatsApp client
		log.Println("Getting QR channel from WhatsApp client...")
		qrChan, err := s.client.GetQRChannel(ctx)
		if err != nil {
			log.Printf("Failed to get QR channel from client: %v", err)
			close(s.qrChan)
			s.qrChan = nil
			return nil, fmt.Errorf("failed to get QR channel: %v", err)
		}
		log.Println("Got QR channel from WhatsApp client successfully")

		// Start QR code handler
		go func() {
			defer func() {
				log.Println("Closing QR channel...")
				close(s.qrChan)
				s.qrChan = nil
				log.Println("QR channel closed")
			}()

			log.Println("Starting QR code handler loop...")
			for evt := range qrChan {
				if evt.Event == "code" {
					log.Printf("Received QR code event, code length: %d", len(evt.Code))
					select {
					case s.qrChan <- evt.Code:
						log.Println("Successfully sent QR code to channel")
					case <-ctx.Done():
						log.Println("Context canceled while sending QR code")
						return
					}
				} else {
					log.Printf("Received QR event: %s", evt.Event)
				}
			}
			log.Println("QR code handler loop finished")
		}()

		// Connect in background
		go func() {
			log.Println("Connecting to WhatsApp in background...")
			if err := s.client.Connect(); err != nil {
				log.Printf("Failed to connect: %v", err)
				return
			}
			log.Println("Connected to WhatsApp successfully")
		}()
	} else {
		log.Println("QR channel already exists")
	}

	log.Println("Returning QR channel")
	return s.qrChan, nil
}

func (s *WhatsAppService) IsLoggedIn() bool {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.client == nil || s.client.Store == nil {
		return false
	}

	isLoggedIn := s.client.Store.ID != nil
	log.Printf("IsLoggedIn check: %v", isLoggedIn)
	return isLoggedIn
}

func (s *WhatsAppService) IsConnected() bool {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.client == nil {
		return false
	}

	isConnected := s.client.IsConnected()
	log.Printf("IsConnected check: %v", isConnected)
	return isConnected
}

func (s *WhatsAppService) Disconnect() error {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.client != nil {
		log.Println("Disconnecting WhatsApp client...")
		s.client.Disconnect()
		s.client = nil
	}
	return nil
}

func (s *WhatsAppService) SendMessage(recipient string, content string) error {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.client == nil {
		return fmt.Errorf("client not initialized")
	}

	// Parse JID
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
		_, err = s.client.SendMessage(context.Background(), jid, msg)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("Attempt %d: Failed to send message: %v", i+1, err)
		time.Sleep(time.Second * time.Duration(i+1))
	}

	return fmt.Errorf("failed to send message after 3 attempts: %v", lastErr)
}

func (s *WhatsAppService) SendFile(recipient string, filePath string, fileName string, fileType string) error {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.client == nil {
		return fmt.Errorf("client not initialized")
	}

	recipient = strings.TrimSpace(recipient)
	if !strings.HasSuffix(recipient, "@s.whatsapp.net") {
		recipient = recipient + "@s.whatsapp.net"
	}

	jid, err := types.ParseJID(recipient)
	if err != nil {
		return fmt.Errorf("invalid phone number: %v", err)
	}

	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	// Calculate SHA256
	sha256sum := sha256.Sum256(data)

	// Determine message type and create appropriate message
	var msg *waProto.Message
	switch {
	case strings.HasPrefix(fileType, "image/"):
		msg = &waProto.Message{
			ImageMessage: &waProto.ImageMessage{
				URL:           proto.String(filePath),
				Mimetype:      proto.String(fileType),
				Caption:       proto.String(fileName),
				FileLength:    proto.Uint64(uint64(len(data))),
				FileSHA256:    sha256sum[:],
				JPEGThumbnail: nil, // You might want to generate a thumbnail
			},
		}
	case strings.HasPrefix(fileType, "video/"):
		msg = &waProto.Message{
			VideoMessage: &waProto.VideoMessage{
				URL:        proto.String(filePath),
				Mimetype:   proto.String(fileType),
				Caption:    proto.String(fileName),
				FileLength: proto.Uint64(uint64(len(data))),
				FileSHA256: sha256sum[:],
			},
		}
	case strings.HasPrefix(fileType, "audio/"):
		msg = &waProto.Message{
			AudioMessage: &waProto.AudioMessage{
				URL:        proto.String(filePath),
				Mimetype:   proto.String(fileType),
				FileLength: proto.Uint64(uint64(len(data))),
				FileSHA256: sha256sum[:],
				Seconds:    proto.Uint32(0),   // Duration in seconds
				PTT:        proto.Bool(false), // Push to talk
			},
		}
	default:
		msg = &waProto.Message{
			DocumentMessage: &waProto.DocumentMessage{
				URL:        proto.String(filePath),
				Mimetype:   proto.String(fileType),
				Title:      proto.String(fileName),
				FileLength: proto.Uint64(uint64(len(data))),
				FileSHA256: sha256sum[:],
			},
		}
	}

	// Add retry logic for sending files
	var lastErr error
	for i := 0; i < 3; i++ {
		_, err = s.client.SendMessage(context.Background(), jid, msg)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("Attempt %d: Failed to send file: %v", i+1, err)
		time.Sleep(time.Second * time.Duration(i+1))
	}

	return fmt.Errorf("failed to send file after 3 attempts: %v", lastErr)
}

func (s *WhatsAppService) SetMessageHandler(handler func(*domain.Message)) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	s.handler = handler
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
