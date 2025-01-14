# WhatSApp Client

A modern WhatsApp client implementation using Go for the backend and Rust for the CLI client.

## Features

- 🔐 Secure WhatsApp connection using official WhatsApp Web protocol
- 📱 Easy QR code scanning for authentication
- 💬 Send and receive text messages
- 📎 Send files with captions
- 🔄 Automatic session management
- 🌐 WebSocket-based communication between server and client
- 📝 Detailed logging for troubleshooting

## Prerequisites

- Go 1.21 or later
- Rust 1.75 or later
- SQLite3

## Installation

### Server (Go)

1. Clone the repository:
```bash
git clone https://github.com/sabry-awad97/whatsapp.git
cd whatsapp
```

2. Install Go dependencies:
```bash
go mod download
```

3. Build the server:
```bash
go build -o whatsapp-server cmd/main.go
```

### Client (Rust)

1. Navigate to the client directory:
```bash
cd whatsapp-client
```

2. Build the client:
```bash
cargo build --release
```

## Usage

### Starting the Server

1. Run the server:
```bash
./whatsapp-server
```

The server will start on `localhost:8080` by default.

### Using the Client

1. Run the client:
```bash
./target/release/whatsapp-client
```

2. If this is your first time:
   - A QR code will be displayed in the terminal
   - Scan it with WhatsApp on your phone
   - Follow the on-screen instructions

3. If you've connected before:
   - The client will automatically reconnect using your saved session
   - You'll see the menu immediately

### Available Commands

- Send Message: Send a text message to a contact
- Send File: Send a file with an optional caption
- Exit: Close the client

## Architecture

### Backend (Go)

- Uses `whatsmeow` library for WhatsApp protocol implementation
- WebSocket server for real-time communication
- SQLite for session storage
- Clean architecture with domain-driven design

### Client (Rust)

- Terminal-based user interface
- Async WebSocket client
- QR code rendering in terminal
- Progress indicators for operations

## API Documentation

See [WebSocket API Documentation](docs/websocket-api.md) for detailed API specifications.

## Development

### Running Tests

Server:
```bash
go test ./...
```

Client:
```bash
cargo test
```

### Logging

- Server logs are written to stdout and can be redirected to a file
- Client logs use the RUST_LOG environment variable for configuration

Example:
```bash
RUST_LOG=debug ./target/release/whatsapp-client
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [whatsmeow](https://github.com/tulir/whatsmeow) - Go WhatsApp Web implementation
- [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite) - Rust WebSocket client
- [qr2term](https://github.com/qqself/qr2term-rs) - Terminal QR code renderer
