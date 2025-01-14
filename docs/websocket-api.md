# WhatsApp WebSocket API Documentation

## Overview

This document describes the WebSocket API for interacting with the WhatsApp client. The API allows you to connect to WhatsApp, send messages, and receive real-time updates.

## Connection

### Endpoint

```
ws://<host>/ws
```

### Connection Flow

1. Connect to the WebSocket endpoint
2. Wait for the QR code
3. Scan the QR code with your WhatsApp mobile app
4. Once connected, you can start sending messages

## Message Types

### Server → Client Messages

These are the messages that your client will receive from the server.

#### QR Code
```json
{
    "type": "qr",
    "code": "string"  // QR code data to be displayed/processed
}
```

#### Connection Status
```json
{
    "type": "connected"  // Indicates successful WhatsApp connection
}
```

```json
{
    "type": "disconnected"  // Indicates WhatsApp disconnection
}
```

#### Incoming Message
```json
{
    "type": "message",
    "sender": "string",     // Phone number with @s.whatsapp.net suffix
    "content": "string",    // Message content
    "message_type": "string" // Message type: "text", "image", "video", "audio", "document", "sticker"
}
```

#### Send Response
```json
{
    "type": "send_response",
    "content": "string",    // Response message
    "success": boolean      // true if message was sent successfully
}
```

### Client → Server Messages

These are the messages that your client can send to the server.

#### Send Message
```json
{
    "type": "send_message",
    "recipient": "string",  // Phone number (with or without @s.whatsapp.net)
    "content": "string"     // Message content
}
```

## Message Types Reference

| Type | Description |
|------|-------------|
| `text` | Plain text message |
| `image` | Image with optional caption |
| `video` | Video with optional caption |
| `audio` | Audio message |
| `document` | Document with optional title |
| `sticker` | Sticker message |

## Example Usage

Here are examples of how to use the WebSocket API in different programming languages.

### JavaScript (Browser)
```javascript
const ws = new WebSocket('ws://localhost:8080/ws');

ws.onopen = () => {
    console.log('Connected to WhatsApp server');
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    
    switch (data.type) {
        case 'qr':
            // Display QR code to user
            displayQRCode(data.code);
            break;
        case 'connected':
            console.log('Connected to WhatsApp');
            break;
        case 'message':
            console.log(`Message from ${data.sender}: ${data.content}`);
            break;
        case 'send_response':
            console.log(data.success ? 'Message sent' : 'Failed to send message');
            break;
    }
};

// Send a message
function sendMessage(phoneNumber, content) {
    ws.send(JSON.stringify({
        type: 'send_message',
        recipient: phoneNumber,
        content: content
    }));
}
```

### Python
```python
import websockets
import asyncio
import json

async def connect_whatsapp():
    async with websockets.connect('ws://localhost:8080/ws') as ws:
        # Handle incoming messages
        async for message in ws:
            data = json.loads(message)
            
            if data['type'] == 'qr':
                # Display QR code
                print(f"Scan QR code: {data['code']}")
            elif data['type'] == 'connected':
                print("Connected to WhatsApp")
            elif data['type'] == 'message':
                print(f"Message from {data['sender']}: {data['content']}")
            
        # Send a message
        await ws.send(json.dumps({
            'type': 'send_message',
            'recipient': '+1234567890',
            'content': 'Hello from Python!'
        }))

asyncio.get_event_loop().run_until_complete(connect_whatsapp())
```

### Rust
```rust
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct OutgoingMessage {
    #[serde(rename = "type")]
    message_type: String,
    recipient: String,
    content: String,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum IncomingMessage {
    #[serde(rename = "connected")]
    Connected,
    #[serde(rename = "qr")]
    QR { code: String },
    #[serde(rename = "message")]
    Message {
        sender: String,
        content: String,
        message_type: String,
    },
    #[serde(rename = "send_response")]
    SendResponse {
        content: String,
        success: bool,
    },
}

async fn connect_whatsapp() -> Result<(), Box<dyn std::error::Error>> {
    let (mut ws_stream, _) = connect_async("ws://localhost:8080/ws").await?;
    
    // Send a message
    let message = OutgoingMessage {
        message_type: "send_message".to_string(),
        recipient: "+1234567890".to_string(),
        content: "Hello from Rust!".to_string(),
    };
    ws_stream.send(Message::Text(serde_json::to_string(&message)?)).await?;
    
    // Handle incoming messages
    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            let message: IncomingMessage = serde_json::from_str(&text)?;
            match message {
                IncomingMessage::Connected => println!("Connected to WhatsApp"),
                IncomingMessage::QR { code } => println!("Scan QR code: {}", code),
                IncomingMessage::Message { sender, content, .. } => {
                    println!("Message from {}: {}", sender, content)
                },
                IncomingMessage::SendResponse { content, success } => {
                    println!("{}: {}", if success { "Success" } else { "Error" }, content)
                }
            }
        }
    }
    Ok(())
}
```

## Error Handling

The server will send error responses in the following format:
```json
{
    "type": "send_response",
    "success": false,
    "content": "Error message describing what went wrong"
}
```

Common error scenarios:
- Invalid phone number format
- Message sending failed
- WhatsApp connection lost
- Invalid message format

## Best Practices

1. **Connection Management**
   - Implement reconnection logic with exponential backoff
   - Handle connection errors gracefully
   - Monitor connection state

2. **Message Handling**
   - Validate message content before sending
   - Implement proper error handling
   - Keep track of message status

3. **QR Code**
   - Display QR code clearly to the user
   - Provide clear instructions for scanning
   - Handle QR code refresh/timeout

4. **Security**
   - Use secure WebSocket connections (wss://) in production
   - Validate input data
   - Implement proper authentication if needed

## Rate Limiting

- Implement reasonable delays between messages
- Handle rate limit errors gracefully
- Consider implementing a message queue for high-volume scenarios

## Support

For issues and feature requests, please open an issue in the GitHub repository or contact the development team.
