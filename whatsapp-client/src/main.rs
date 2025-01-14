use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use futures_util::{SinkExt, StreamExt};
use qr2term::print_qr;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

#[derive(Debug, Serialize)]
struct OutgoingMessage {
    #[serde(rename = "type")]
    message_type: String,
    phone: String,
    content: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum IncomingMessage {
    #[serde(rename = "send_response")]
    SendResponse {
        success: bool,
        error: Option<String>,
    },
    #[serde(rename = "received_message")]
    ReceivedMessage { from: String, content: String },
    #[serde(rename = "qr_code")]
    QRCode { qr_code: String },
    #[serde(rename = "error")]
    Error { error: String },
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

async fn handle_incoming_message(text: String) -> Result<()> {
    match serde_json::from_str::<IncomingMessage>(&text) {
        Ok(IncomingMessage::SendResponse { success, error }) => {
            if success {
                println!("{}", "✅ Message sent successfully!".green());
            } else {
                println!(
                    "{}",
                    format!("❌ Failed to send message: {}", error.unwrap_or_default()).red()
                );
            }
        }
        Ok(IncomingMessage::ReceivedMessage { from, content }) => {
            println!(
                "{} {} {}: {}",
                "📱".green(),
                "Message from".blue(),
                from.yellow(),
                content.white()
            );
        }
        Ok(IncomingMessage::QRCode { qr_code }) => {
            println!(
                "\n📱 {}",
                "Scan this QR code with WhatsApp on your phone:".cyan()
            );
            print_qr(&qr_code).expect("Failed to print QR code");
            println!("\n⏳ {}", "Waiting for connection...".yellow());
        }
        Ok(IncomingMessage::Error { error }) => {
            println!("{}", format!("❌ Server error: {}", error).red());
        }
        Err(e) => {
            println!("{}", format!("❌ Failed to parse message: {}", e).red());
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
    let outgoing = OutgoingMessage {
        message_type: "send_message".to_string(),
        phone,
        content,
    };
    let json = serde_json::to_string(&outgoing)?;
    write.send(Message::Text(json.into())).await?;
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
            handle_incoming_message(text.to_string()).await?;
        }
        return Ok(());
    }

    // Start a task to read incoming messages
    let receive_task = tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Err(e) = handle_incoming_message(text.to_string()).await {
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

        if let Err(e) = send_message(&mut write, parts[0].to_string(), parts[1].to_string()).await {
            println!("{}", format!("Failed to send message: {}", e).red());
            break;
        }
    }

    receive_task.abort();
    Ok(())
}
