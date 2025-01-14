# WhatsApp Web Client

A modern, feature-rich WhatsApp client built with Go, featuring a web interface and real-time messaging capabilities.

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat-square&logo=go)](https://golang.org/doc/devel/release.html)
[![License](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)

## Features

- **Web Interface**: Modern web UI for sending and receiving messages
- **Real-time Updates**: Instant message delivery and receipt using WebSocket
- **Secure**: End-to-end encryption using WhatsApp's official protocol
- **Multi-device**: Compatible with WhatsApp's multi-device feature
- **Fast & Lightweight**: Built with Go for optimal performance
- **Message History**: View and manage your chat history

## Quick Start

### Prerequisites

- Go 1.21 or higher
- SQLite3
- Modern web browser

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/sabry-awad97/whatsapp.git
   cd whatsapp
   ```

2. Install dependencies:
   ```bash
   go mod tidy
   ```

3. Run the application:
   ```bash
   go run cmd/main.go
   ```

4. Open your web browser and navigate to:
   ```
   http://localhost:8080
   ```

5. Scan the QR code with WhatsApp on your phone to link your account.

## Architecture

The project follows a clean architecture pattern with the following components:

- `cmd/`: Application entry points
- `internal/`: Internal packages
  - `config/`: Configuration management
  - `domain/`: Core domain types and interfaces
  - `server/`: HTTP and WebSocket servers
  - `service/`: Business logic implementation
- `pkg/`: Reusable packages

## Usage

### Sending Messages

1. Navigate to `http://localhost:8080` in your web browser
2. Enter the recipient's phone number (international format)
3. Type your message
4. Click "Send" or press Enter

### Receiving Messages

Messages are automatically displayed in the web interface as they arrive.

## Configuration

The application can be configured using environment variables or a `.env` file:

```env
WHATSAPP_LOG_LEVEL=INFO
WHATSAPP_DB_PATH=whatsapp.db
WHATSAPP_SERVER_PORT=8080
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [whatsmeow](https://github.com/tulir/whatsmeow) - Go WhatsApp Web implementation
- [gorilla/websocket](https://github.com/gorilla/websocket) - WebSocket implementation for Go
- [qrterminal](https://github.com/mdp/qrterminal) - QR code generator for terminal

## Support

If you have any questions or need help, please:

1. Check the [Issues](https://github.com/yourusername/whatsapp/issues) page
2. Create a new issue if your problem isn't already listed
3. Join our community discussions

---

Made with ❤️ using Go
