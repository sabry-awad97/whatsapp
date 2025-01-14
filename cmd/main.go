package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"whatsapp/internal/server"
	"whatsapp/internal/service"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func main() {
	// Remove existing database if it exists
	dbPath := "whatsapp.db"
	if _, err := os.Stat(dbPath); err == nil {
		if err := os.Remove(dbPath); err != nil {
			log.Fatalf("Failed to remove existing database: %v", err)
		}
	}

	// Create custom store
	store, err := service.NewCustomStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Initialize SQLite store
	dbLog := waLog.Stdout("Database", "INFO", true)
	container := sqlstore.NewWithDB(store.DB, "sqlite", dbLog)
	if err := container.Upgrade(); err != nil {
		log.Fatalf("Failed to upgrade database: %v", err)
	}

	// Initialize WhatsApp service
	whatsapp, err := service.NewWhatsAppService(container)
	if err != nil {
		log.Fatalf("Failed to create WhatsApp service: %v", err)
	}

	// Create WebSocket server
	wsServer := server.NewWebSocketServer(whatsapp)

	// Create HTTP server
	httpServer := server.NewHTTPServer(wsServer)

	// Start HTTP server
	go func() {
		if err := httpServer.Start(":8080"); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	fmt.Println("\n📱 WhatsApp client is ready!")
	fmt.Println("💻 Open http://localhost:8080 in your browser")
	fmt.Println("👋 Press Ctrl+C to exit")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n👋 Shutting down...")
	whatsapp.Disconnect()
}
