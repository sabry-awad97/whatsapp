package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"whatsapp/internal/server"
	"whatsapp/internal/service"
)

func main() {
	// Create context that will be canceled on interrupt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupts
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("\nReceived signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	// Initialize WhatsApp service
	whatsapp, err := service.NewWhatsAppService()
	if err != nil {
		log.Fatalf("Failed to initialize WhatsApp service: %v", err)
	}

	// Create WebSocket server
	ws := server.NewWebSocketServer(whatsapp)

	// Configure HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.HandleWebSocket)
	mux.Handle("/", http.FileServer(http.Dir("web")))

	// Create HTTP server
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start HTTP server
	go func() {
		log.Printf("Starting server on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
			cancel()
		}
	}()

	// Print welcome message
	fmt.Println("\n📱 WhatsApp client is ready!")
	fmt.Printf("💻 Open http://localhost%s in your browser\n", srv.Addr)
	fmt.Println("👋 Press Ctrl+C to exit")

	// Wait for context cancellation
	<-ctx.Done()

	// Initiate graceful shutdown
	log.Println("Shutting down services...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	} else {
		log.Println("HTTP server shutdown complete")
	}

	// Disconnect WhatsApp client
	if err := whatsapp.Disconnect(); err != nil {
		log.Printf("WhatsApp client disconnect error: %v", err)
	} else {
		log.Println("WhatsApp client disconnected")
	}

	log.Println("Shutdown complete 👋")
}
