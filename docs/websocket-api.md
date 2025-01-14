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

#### Send File
```json
{
    "type": "send_file",
    "recipient": "string",  // Phone number (with or without @s.whatsapp.net)
    "fileName": "string",   // Name of the file
    "fileData": "string",   // Base64 encoded file data
    "fileType": "string",   // MIME type (e.g., "image/jpeg", "application/pdf")
    "content": "string"     // Optional caption for the file
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

## File Type Support

The API supports various file types:

### Images
- MIME types: image/jpeg, image/png, image/gif
- Maximum size: 16MB
- Supported formats: JPG, PNG, GIF

### Videos
- MIME types: video/mp4, video/3gpp
- Maximum size: 16MB
- Supported formats: MP4, 3GP

### Audio
- MIME types: audio/mp3, audio/ogg, audio/wav
- Maximum size: 16MB
- Supported formats: MP3, OGG, WAV

### Documents
- Various MIME types supported
- Maximum size: 100MB
- Common formats: PDF, DOC, DOCX, XLS, XLSX, etc.

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

// Function to send a file
async function sendFile(phoneNumber, file, caption = '') {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = async function(e) {
            const base64Data = e.target.result.split(',')[1];
            const data = {
                type: 'send_file',
                recipient: phoneNumber,
                fileName: file.name,
                fileData: base64Data,
                fileType: file.type,
                content: caption
            };
            
            try {
                ws.send(JSON.stringify(data));
                resolve();
            } catch (error) {
                reject(error);
            }
        };
        reader.onerror = reject;
        reader.readAsDataURL(file);
    });
}

// Example usage
const fileInput = document.querySelector('input[type="file"]');
fileInput.addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (file) {
        try {
            await sendFile('+1234567890', file, 'Check out this file!');
            console.log('File sent successfully');
        } catch (error) {
            console.error('Failed to send file:', error);
        }
    }
});
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

async def send_file(ws, phone_number, file_path, caption=''):
    # Read file and get MIME type
    with open(file_path, 'rb') as f:
        file_data = f.read()
    mime_type = mimetypes.guess_type(file_path)[0]
    
    # Encode file data
    base64_data = base64.b64encode(file_data).decode('utf-8')
    
    # Create message
    message = {
        'type': 'send_file',
        'recipient': phone_number,
        'fileName': file_path.split('/')[-1],
        'fileData': base64_data,
        'fileType': mime_type,
        'content': caption
    }
    
    # Send message
    await ws.send(json.dumps(message))

# Example usage
async def main():
    async with websockets.connect('ws://localhost:8080/ws') as ws:
        await send_file(ws, '+1234567890', 'path/to/file.jpg', 'Check this out!')

asyncio.get_event_loop().run_until_complete(main())
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

#[derive(Serialize)]
struct FileMessage {
    #[serde(rename = "type")]
    message_type: String,
    recipient: String,
    file_name: String,
    file_data: String,
    file_type: String,
    content: Option<String>,
}

async fn send_file(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    phone: &str,
    file_path: &Path,
    caption: Option<String>,
) -> Result<()> {
    // Read file
    let mut file = File::open(file_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    // Get file name and type
    let file_name = file_path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_string();
    
    let file_type = mime_guess::from_path(file_path)
        .first_or_octet_stream()
        .to_string();
    
    // Encode file data
    let file_data = BASE64.encode(&buffer);
    
    // Create message
    let message = FileMessage {
        message_type: "send_file".to_string(),
        recipient: phone.to_string(),
        file_name,
        file_data,
        file_type,
        content: caption,
    };
    
    // Send message
    ws.send(Message::Text(serde_json::to_string(&message)?)).await?;
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

## Best Practices for File Handling

1. **File Size**
   - Check file size before sending
   - Consider compressing large files
   - Split very large files into smaller chunks

2. **File Types**
   - Verify file type before sending
   - Use correct MIME types
   - Handle unsupported file types gracefully

3. **Error Handling**
   - Handle file read errors
   - Handle encoding errors
   - Implement proper timeout handling

4. **Performance**
   - Use async file operations
   - Consider implementing progress indicators
   - Implement retry logic for large files

## Rate Limiting

- Implement reasonable delays between messages
- Handle rate limit errors gracefully
- Consider implementing a message queue for high-volume scenarios

## Support

For issues and feature requests, please open an issue in the GitHub repository or contact the development team.
