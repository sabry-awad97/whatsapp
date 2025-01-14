use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use console::{style, Term};
use dialoguer::{theme::ColorfulTheme, Confirm, Input, Select};
use futures_util::{Sink, SinkExt, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use qr2term::print_qr;
use serde::Deserialize;
use serde_json::json;
use std::error::Error as StdError;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::{net::TcpStream, time::sleep};
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

const BANNER: &str = r#"
__          ___           _                            
\ \        / / |         | |         /\                
 \ \  /\  / /| |__   __ _| |_ ___   /  \   _ __  _ __  
  \ \/  \/ / | '_ \ / _` | __/ __| / /\ \ | '_ \| '_ \ 
   \  /\  /  | | | | (_| | |_\__ \/ ____ \| |_) | |_) |
    \/  \/   |_| |_|\__,_|\__|___/_/    \_\ .__/| .__/ 
                                          | |   | |    
                                          |_|   |_|    
"#;

const QR_INSTRUCTIONS: &str = r#"
┌──────────────────────────────────────────────────┐
│  1. Open WhatsApp on your phone                  │
│  2. Tap Menu or Settings and select WhatsApp Web │
│  3. Point your phone at this screen to scan      │
│  4. QR code will disappear when scan completes   │
└──────────────────────────────────────────────────┘
"#;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ServerMessage {
    #[serde(rename = "qr")]
    QR { code: String },
    #[serde(rename = "connected")]
    Connected,
    #[serde(rename = "disconnected")]
    Disconnected,
    #[serde(rename = "message")]
    Message {
        sender: String,
        content: String,
        #[serde(rename = "message_type")]
        message_type: String,
    },
    #[serde(rename = "send_response")]
    SendResponse {
        content: String,
        #[serde(default)]
        success: bool,
    },
    #[serde(rename = "error")]
    Error { content: String },
}

#[derive(Debug, Deserialize)]
struct WebSocketMessage {
    msg_type: String,
    code: Option<String>,
    content: Option<String>,
}

async fn connect_websocket() -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<dyn StdError>>
{
    let url = "ws://localhost:8080/ws";
    info!("Connecting to WebSocket server at {}", url);
    let (ws_stream, _) = connect_async(url).await?;
    info!("WebSocket connection established");
    Ok(ws_stream)
}

async fn wait_for_qr_and_connect(
    mut ws_stream: impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
) -> Result<bool, Box<dyn StdError>> {
    let term = Term::stdout();
    term.clear_screen()?;

    // Print banner
    println!("{}", style(BANNER).cyan().bold());

    let mut spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner} {msg}")?,
    );
    spinner.set_message("Waiting for QR code...");
    info!("Waiting for QR code from server");

    let mut qr_received = false;
    let mut connected = false;
    let mut early_connection = false;

    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            debug!("Received WebSocket message: {}", text);
            let server_msg: ServerMessage = serde_json::from_str(&text)?;
            match server_msg {
                ServerMessage::QR { code } => {
                    if connected {
                        warn!("Received QR code after connection, ignoring");
                        continue;
                    }
                    qr_received = true;
                    info!("Received QR code, length: {}", code.len());
                    spinner.finish_and_clear();

                    // Clear screen and show instructions
                    term.clear_screen()?;
                    println!("{}", style(BANNER).cyan().bold());
                    println!("{}", style(QR_INSTRUCTIONS).yellow());

                    let qr_data = code;

                    // Print QR code
                    match print_qr(&qr_data) {
                        Ok(_) => {
                            info!("QR code displayed successfully");
                            println!(
                                "\n{}",
                                style("Scan the QR code above with WhatsApp on your phone").green()
                            );

                            // Show waiting message
                            spinner = ProgressBar::new_spinner();
                            spinner.set_style(
                                ProgressStyle::default_spinner()
                                    .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                                    .template("{spinner} {msg}")?,
                            );
                            spinner.set_message("Waiting for scan...");

                            // If we received a connection message earlier, process it now
                            if early_connection {
                                debug!("Processing early connection message");
                                connected = true;
                                info!("Connection confirmed after QR code scan");
                                spinner.finish_with_message("✓ Connected successfully!");
                                // Wait a bit for the connection to fully establish
                                info!("Waiting for connection to stabilize...");
                                sleep(Duration::from_secs(2)).await;
                                info!("Connection established successfully");
                                return Ok(true);
                            }
                        }
                        Err(e) => {
                            error!("Failed to display QR code: {}", e);
                            println!("{}", style("Error displaying QR code:").red().bold());
                            println!("{}", e);
                            println!("\n{}", style("Raw QR data:").yellow().bold());
                            println!("{}", qr_data);
                            return Ok(false);
                        }
                    }
                }
                ServerMessage::Connected => {
                    if connected {
                        debug!("Received duplicate connected message");
                        continue;
                    }
                    if !qr_received {
                        debug!("Received early connection message, storing for later");
                        early_connection = true;
                        continue;
                    }
                    connected = true;
                    info!("Received connection confirmation");
                    spinner.finish_with_message("✓ Connected successfully!");
                    // Wait a bit for the connection to fully establish
                    info!("Waiting for connection to stabilize...");
                    sleep(Duration::from_secs(2)).await;
                    info!("Connection established successfully");
                    return Ok(true);
                }
                ServerMessage::Message {
                    sender,
                    content,
                    message_type,
                } => {
                    if !connected {
                        debug!(
                            "Received message during connection: {} from {}",
                            content, sender
                        );
                        continue;
                    }
                    info!(
                        "Message from {}: {} (type: {})",
                        sender, content, message_type
                    );
                }
                ServerMessage::Error { content } => {
                    error!("Server error: {}", content);
                    spinner.finish_with_message(format!("✗ Error: {}", content));
                    return Ok(false);
                }
                ServerMessage::Disconnected => {
                    error!("Received disconnection message");
                    spinner.finish_with_message("✗ Connection failed!");
                    return Ok(false);
                }
                _ => {
                    debug!("Ignoring message: {:?}", server_msg);
                    continue;
                }
            }
        }
    }
    warn!("WebSocket stream ended unexpectedly");
    Ok(false)
}

async fn send_message(
    ws_stream: &mut (impl Sink<Message, Error = WebSocketError> + Unpin),
    recipient: String,
    content: String,
) -> Result<(), Box<dyn StdError>> {
    info!("Sending message to {}", recipient);
    debug!("Message content: {}", content);

    let msg = json!({
        "type": "send_message",
        "recipient": recipient,
        "content": content,
    });

    debug!("Sending message JSON: {}", msg.to_string());
    ws_stream
        .send(Message::Text(msg.to_string().into()))
        .await?;
    info!("Message sent successfully");
    Ok(())
}

async fn send_file(
    ws_stream: &mut (impl Sink<Message, Error = WebSocketError> + Unpin),
    recipient: String,
    file_path: PathBuf,
    caption: String,
) -> Result<(), Box<dyn StdError>> {
    info!("Sending file to {}", recipient);
    debug!("File path: {}", file_path.display());

    // Read file
    let file_data = tokio::fs::read(&file_path).await?;
    let file_name = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("Invalid file name")?;

    // Get file type
    let file_type = mime_guess::from_path(&file_path)
        .first_or_octet_stream()
        .to_string();

    debug!("File type: {}", file_type);

    // Encode file data
    let encoded_data = BASE64.encode(&file_data);

    let msg = json!({
        "type": "send_file",
        "recipient": recipient,
        "file_data": encoded_data,
        "file_name": file_name,
        "file_type": file_type,
        "caption": caption,
    });

    debug!("Sending file message");
    ws_stream
        .send(Message::Text(msg.to_string().into()))
        .await?;
    info!("File sent successfully");
    Ok(())
}

async fn handle_send_response(
    ws_stream: &mut (impl StreamExt<Item = Result<Message, WebSocketError>> + Unpin),
    spinner: &ProgressBar,
    recipient: &str,
) -> Result<bool, Box<dyn StdError>> {
    info!("Waiting for send confirmation");
    let mut success = false;
    let mut response_received = false;

    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            debug!("Received response: {}", text);
            let server_msg: ServerMessage = serde_json::from_str(&text)?;
            match server_msg {
                ServerMessage::SendResponse {
                    content,
                    success: is_success,
                } => {
                    if is_success {
                        info!("Message sent successfully: {}", content);
                    } else {
                        error!("Failed to send message: {}", content);
                    }
                    spinner.finish_with_message(if is_success {
                        format!("✓ Message sent to {}", recipient)
                    } else {
                        format!("✗ Failed to send message: {}", content)
                    });
                    success = is_success;
                    response_received = true;
                    break;
                }
                ServerMessage::Message {
                    sender,
                    content,
                    message_type,
                } => {
                    debug!(
                        "Received message during send: {} from {} (type: {})",
                        content, sender, message_type
                    );
                    continue;
                }
                ServerMessage::Disconnected => {
                    error!("Connection lost while waiting for send confirmation");
                    spinner.finish_with_message("✗ Connection lost!");
                    return Ok(false);
                }
                _ => continue,
            }
        }
    }

    if !response_received {
        warn!("No send response received");
        spinner.finish_with_message("✗ No confirmation received");
        return Ok(false);
    }

    Ok(success)
}

async fn handle_websocket_messages(
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    early_connected: Arc<AtomicBool>,
) -> Result<(), Box<dyn StdError>> {
    let mut qr_spinner = ProgressBar::new_spinner();
    qr_spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner} {msg}")?,
    );
    qr_spinner.set_message("Waiting for QR code...");
    info!("Waiting for QR code from server");

    let mut qr_received = false;

    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            debug!("Received WebSocket message: {}", text);
            let message: WebSocketMessage = serde_json::from_str(&text)?;

            match message.msg_type.as_str() {
                "qr" => {
                    if let Some(code) = message.code {
                        debug!("Received QR code, length: {}", code.len());
                        qr_spinner.finish_and_clear();
                        let _ = print_qr(&code);
                        qr_received = true;
                        qr_spinner = ProgressBar::new_spinner();
                        qr_spinner.set_style(
                            ProgressStyle::default_spinner()
                                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                                .template("{spinner} {msg}")?,
                        );
                        qr_spinner.set_message("Waiting for connection...");
                    }
                }
                "connected" => {
                    if qr_received {
                        qr_spinner.finish_with_message("✅ Connected to WhatsApp!\n");
                    } else {
                        debug!("Already connected, no QR needed");
                        early_connected.store(true, Ordering::SeqCst);
                    }
                    break;
                }
                "error" => {
                    if let Some(content) = message.content {
                        error!("Received error: {}", content);
                        qr_spinner.finish_with_message(format!("❌ Error: {}\n", content));
                    }
                    break;
                }
                _ => {
                    debug!("Received unknown message type: {}", message.msg_type);
                }
            }
        }
    }

    qr_spinner.finish_and_clear();
    Ok(())
}

async fn wait_for_connection(
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<bool, Box<dyn StdError>> {
    let early_connected = Arc::new(AtomicBool::new(false));
    handle_websocket_messages(ws_stream, early_connected.clone()).await?;

    if early_connected.load(Ordering::SeqCst) {
        info!("Already connected, proceeding with menu");
        return Ok(true);
    }

    Ok(false)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // Initialize tracing subscriber
    FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("whatsapp_client=debug".parse()?),
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_level(true)
        .with_ansi(true)
        .pretty()
        .try_init()
        .map_err(|e| format!("Failed to initialize logging: {}", e))?;

    info!("Starting WhatsApp client");
    debug!("Initializing UI components");

    let theme = ColorfulTheme::default();
    let term = Term::stdout();
    term.clear_screen()?;

    println!("{}", style(BANNER).cyan().bold());
    println!("{}", style("=================").cyan());
    println!();

    info!("Connecting to WebSocket server");
    let mut ws_stream = connect_websocket().await?;

    let connected = wait_for_connection(&mut ws_stream).await?;

    if !connected {
        info!("Waiting for QR code and connection");
        let connected = wait_for_qr_and_connect(&mut ws_stream).await?;
        if !connected {
            error!("Failed to establish WhatsApp connection");
            println!(
                "\n{}",
                style("Failed to connect to WhatsApp. Please try again.")
                    .red()
                    .bold()
            );
            return Ok(());
        }
    }

    println!("\n{}", style("Connected to WhatsApp!").green().bold());
    sleep(Duration::from_secs(1)).await;

    info!("Starting main interaction loop");
    loop {
        term.clear_screen()?;
        println!("{}", style(BANNER).cyan().bold());
        println!("{}", style("=================").cyan());
        println!();

        println!("{}", style("🟢 Connected to WhatsApp").green().bold());
        println!();

        let options = vec!["📱 Send Message", "📎 Send File", "❌ Exit"];
        let selection = Select::with_theme(&theme)
            .with_prompt("What would you like to do?")
            .default(0)
            .items(&options)
            .interact()?;

        match selection {
            0 => {
                debug!("User selected: Send Message");
                let recipient: String = Input::with_theme(&theme)
                    .with_prompt("Enter recipient's phone number")
                    .validate_with(|input: &String| -> Result<(), &str> {
                        if input.chars().all(|c| c.is_ascii_digit() || c == '+') {
                            Ok(())
                        } else {
                            Err("Phone number should only contain digits and '+' symbol")
                        }
                    })
                    .interact_text()?;

                let content: String = Input::with_theme(&theme)
                    .with_prompt("Enter your message")
                    .interact_text()?;

                let spinner = ProgressBar::new_spinner();
                spinner.set_style(
                    ProgressStyle::default_spinner()
                        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                        .template("{spinner} {msg}")?,
                );
                spinner.set_message("Sending message...");

                send_message(&mut ws_stream, recipient.clone(), content).await?;

                // Wait for send response
                if !handle_send_response(&mut ws_stream, &spinner, &recipient).await? {
                    continue;
                }

                sleep(Duration::from_secs(1)).await;
            }
            1 => {
                debug!("User selected: Send File");
                let recipient: String = Input::with_theme(&theme)
                    .with_prompt("Enter recipient's phone number")
                    .validate_with(|input: &String| -> Result<(), &str> {
                        if input.chars().all(|c| c.is_ascii_digit() || c == '+') {
                            Ok(())
                        } else {
                            Err("Phone number should only contain digits and '+' symbol")
                        }
                    })
                    .interact_text()?;

                let file_path: String = Input::with_theme(&theme)
                    .with_prompt("Enter file path")
                    .validate_with(|input: &String| -> Result<(), &str> {
                        let path = PathBuf::from(input);
                        if path.exists() {
                            Ok(())
                        } else {
                            Err("File does not exist")
                        }
                    })
                    .interact_text()?;

                let caption: String = Input::with_theme(&theme)
                    .with_prompt("Enter caption (optional)")
                    .allow_empty(true)
                    .interact_text()?;

                let spinner = ProgressBar::new_spinner();
                spinner.set_style(
                    ProgressStyle::default_spinner()
                        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                        .template("{spinner} {msg}")?,
                );
                spinner.set_message("Sending file...");

                send_file(
                    &mut ws_stream,
                    recipient.clone(),
                    PathBuf::from(&file_path),
                    caption,
                )
                .await?;

                // Wait for send response
                if !handle_send_response(&mut ws_stream, &spinner, &recipient).await? {
                    continue;
                }

                sleep(Duration::from_secs(1)).await;
            }
            2 => {
                debug!("User selected: Exit");
                if Confirm::with_theme(&theme)
                    .with_prompt("Are you sure you want to exit?")
                    .default(false)
                    .interact()?
                {
                    info!("User confirmed exit");
                    println!("\n{}", style("Goodbye!").cyan().bold());
                    break;
                }
            }
            _ => unreachable!(),
        }
    }

    info!("WhatsApp client shutting down");
    Ok(())
}
