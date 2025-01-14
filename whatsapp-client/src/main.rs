use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use futures_util::{SinkExt, StreamExt};
use qr2term::print_qr;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

#[derive(Debug, Serialize)]
struct OutgoingMessage {
    #[serde(rename = "type")]
    message_type: String,
    recipient: String,
    content: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum IncomingMessage {
    #[serde(rename = "connected")]
    Connected,
    #[serde(rename = "qr")]
    QR { code: String },
    #[serde(rename = "disconnected")]
    Disconnected,
    #[serde(rename = "message")]
    Message {
        sender: String,
        content: String,
        #[serde(default)]
        message_type: String,
    },
    #[serde(rename = "send_response")]
    SendResponse {
        content: String,
        #[serde(default)]
        success: bool,
    },
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// WebSocket server URL
    #[arg(short, long, default_value = "ws://localhost:8080/ws")]
    url: String,

    /// Phone number to send message to (optional)
    #[arg(short, long)]
    phone: Option<String>,

    /// Message to send (optional)
    #[arg(short, long)]
    message: Option<String>,
}

async fn handle_incoming_message(msg: IncomingMessage) -> Result<()> {
    match msg {
        IncomingMessage::Connected => {
            println!("{}", "✅ Connected to WhatsApp!".green());
        }
        IncomingMessage::QR { code } => {
            println!(
                "\n📱 {}",
                "Scan this QR code with WhatsApp on your phone:".cyan()
            );
            print_qr(&code)?;
            println!("\n⏳ {}", "Waiting for connection...".yellow());
        }
        IncomingMessage::Disconnected => {
            println!("{}", "❌ Disconnected from WhatsApp!".red());
        }
        IncomingMessage::Message {
            sender,
            content,
            message_type,
        } => {
            let icon = match message_type.as_str() {
                "image" => "🖼️",
                "video" => "🎥",
                "audio" => "🎵",
                "document" => "📄",
                "sticker" => "🎨",
                _ => "📱",
            };
            println!(
                "{} Message from {}: {}",
                icon,
                sender.yellow(),
                content.white()
            );
        }
        IncomingMessage::SendResponse { content, success } => {
            if success {
                println!("{} {}", "✅".green(), content.green());
            } else {
                println!("{} {}", "❌".red(), content.red());
            }
        }
    }
    Ok(())
}

async fn send_message(
    write: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    phone: String,
    content: String,
) -> Result<()> {
    println!("📤 Sending message to {}...", phone.yellow());
    let outgoing = OutgoingMessage {
        message_type: "send_message".to_string(),
        recipient: phone,
        content,
    };
    let json = serde_json::to_string(&outgoing)?;
    println!("📨 Message payload: {}", json);
    write.send(Message::Text(json.into())).await?;
    println!("✈️ Message sent to server, waiting for response...");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let url = Url::parse(&args.url).context("Failed to parse URL")?;

    println!("{}", "Connecting to WebSocket server...".yellow());
    let (ws_stream, _) = connect_async(url.as_str())
        .await
        .context("Failed to connect")?;
    println!("{}", "Connected!".green());

    let (mut write, mut read) = ws_stream.split();

    // If phone and message are provided as arguments, send them immediately
    if let (Some(phone), Some(msg)) = (args.phone, args.message) {
        send_message(&mut write, phone, msg).await?;

        // Wait for response with timeout
        let response = tokio::time::timeout(Duration::from_secs(10), read.next())
            .await
            .context("Timeout waiting for response")?;

        if let Some(Ok(Message::Text(text))) = response {
            let msg: IncomingMessage = serde_json::from_str(&text)?;
            handle_incoming_message(msg).await?;
        }
        return Ok(());
    }

    // Start a task to read incoming messages
    let receive_task = tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let msg: IncomingMessage = serde_json::from_str(&text).unwrap();
                    if let Err(e) = handle_incoming_message(msg).await {
                        eprintln!("Error handling message: {}", e);
                    }
                }
                Ok(Message::Close(_)) => {
                    println!("{}", "Connection closed by server".red());
                    break;
                }
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Read user input and send messages
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    println!(
        "{}",
        "\nEnter messages in format: <phone_number> <message>".cyan()
    );
    println!("{}", "Example: +1234567890 Hello, World!".cyan());
    println!("{}", "Press Ctrl+C to exit\n".cyan());

    while let Some(line) = lines.next_line().await? {
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        if parts.len() != 2 {
            println!("{}", "Invalid format! Use: <phone_number> <message>".red());
            continue;
        }

        let mut phone = parts[0].to_string();
        // Remove "@s.whatsapp.net" if present
        if let Some(idx) = phone.find('@') {
            phone = phone[..idx].to_string();
        }
        // Add "@s.whatsapp.net" if not present
        if !phone.contains('@') {
            phone = format!("{}@s.whatsapp.net", phone);
        }

        let message = parts[1].to_string();
        println!("📱 Preparing to send message:");
        println!("   To: {}", phone.yellow());
        println!("   Content: {}", message.white());

        match send_message(&mut write, phone, message).await {
            Ok(_) => (),
            Err(e) => {
                println!("{}", format!("❌ Failed to send message: {}", e).red());
                break;
            }
        }
    }

    receive_task.abort();
    Ok(())
}
