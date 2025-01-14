package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"whatsapp/internal/config"
	"whatsapp/internal/domain"
	"whatsapp/internal/server"
	"whatsapp/internal/service"
	"whatsapp/pkg/logger"

	"github.com/mdp/qrterminal/v3"
)

func main() {
	log := logger.New()
	cfg := config.Load()

	// Initialize WhatsApp service
	whatsapp, err := service.NewWhatsAppService(cfg.Client)
	if err != nil {
		log.Error("Failed to create WhatsApp service: %v", err)
		os.Exit(1)
	}

	// Create WebSocket server
	wsServer := server.NewWebSocketServer(whatsapp)
	httpServer := server.NewHTTPServer(wsServer)

	// Set up message handler
	whatsapp.SetMessageHandler(func(msg *domain.Message) {
		wsServer.BroadcastIncomingMessage(msg)
		log.Info("Received message from %s: %s", msg.Sender, msg.Content)
	})

	// Start HTTP server
	go func() {
		if err := httpServer.Start(":8080"); err != nil {
			log.Error("Server error: %v", err)
			os.Exit(1)
		}
	}()

	fmt.Println("\n🌐 Web interface available at http://localhost:8080")
	fmt.Println("📱 Starting WhatsApp client...")

	// Connect to WhatsApp
	ctx := context.Background()
	if err := whatsapp.Connect(ctx); err != nil {
		log.Error("Failed to connect: %v", err)
		os.Exit(1)
	}

	// Handle QR code if needed
	if !whatsapp.IsConnected() {
		fmt.Println("\n⌛ Waiting for QR code...")
		qr, err := whatsapp.GetQR()
		if err != nil {
			log.Error("Failed to get QR code: %v", err)
			os.Exit(1)
		}

		fmt.Println("\n📱 Scan this QR code with WhatsApp on your phone:")
		qrterminal.GenerateHalfBlock(qr, qrterminal.L, os.Stdout)

		fmt.Print("\n⏳ Waiting for connection")

		// Wait for connection
		for i := 0; i < 120; i++ {
			if whatsapp.IsConnected() {
				fmt.Println("\n\n✅ Successfully connected to WhatsApp!")
				break
			}
			if i%3 == 0 {
				fmt.Print(".")
			}
			time.Sleep(time.Second)
		}

		if !whatsapp.IsConnected() {
			fmt.Println("\n\n❌ Connection timed out. Please try again.")
			os.Exit(1)
		}
	} else {
		fmt.Println("✅ Successfully connected to WhatsApp!")
	}

	fmt.Println("\n🚀 System is ready!")
	fmt.Println("💻 Visit http://localhost:8080 to send messages")
	fmt.Println("👋 Press Ctrl+C to exit")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	fmt.Println("\n👋 Shutting down...")
	if err := whatsapp.Disconnect(); err != nil {
		log.Error("Error during shutdown: %v", err)
	}
}
