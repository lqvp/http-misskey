use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

const MISSKEY_DOMAIN: &str = "misskey.io";
const MAX_RECONNECT_ATTEMPTS: usize = 10;
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize, Clone)]
pub struct MisskeyUser {
    pub name: String,
    pub username: String,
    #[serde(rename = "avatarUrl")]
    pub avatar_url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MisskeyNote {
    pub id: String,
    pub text: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    pub user: MisskeyUser,
}

#[derive(Debug, Serialize)]
struct ConnectMessage {
    #[serde(rename = "type")]
    typ: String,
    body: ConnectBody,
}

#[derive(Debug, Serialize)]
struct ConnectBody {
    channel: String,
    id: String,
    params: Value,
}

#[derive(Debug, Serialize)]
struct PingMessage {
    #[serde(rename = "type")]
    typ: String,
}

pub async fn start_misskey_ltl_broadcast(tx: broadcast::Sender<MisskeyNote>) -> JoinHandle<()> {
    let (internal_tx, mut internal_rx) = mpsc::channel::<MisskeyNote>(20);

    let misskey_task = tokio::spawn(async move {
        info!("Misskeyブロードキャストサービスを開始");
        connect_misskey_ltl(internal_tx, None).await;
    });

    // 受信したノートをブロードキャストするタスク
    tokio::spawn(async move {
        info!("ノートブロードキャスターを開始");

        while let Some(note) = internal_rx.recv().await {
            if let Err(e) = tx.send(note) {
                error!("ノートブロードキャストエラー: {}", e);
            }
        }

        info!("ノートブロードキャスター終了");
    });

    misskey_task
}

pub async fn connect_misskey_ltl(tx: mpsc::Sender<MisskeyNote>, token: Option<String>) {
    let mut reconnect_attempts = 0;
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

    loop {
        info!("Misskeyに接続中...");

        let ws_url = match &token {
            Some(t) => format!("wss://{}/streaming?i={}", MISSKEY_DOMAIN, t),
            None => format!("wss://{}/streaming", MISSKEY_DOMAIN),
        };

        let connection_start = Instant::now();

        match connect_async(&ws_url).await {
            Ok((ws_stream, _)) => {
                info!("WebSocket接続成功");

                reconnect_attempts = 0;
                reconnect_delay = INITIAL_RECONNECT_DELAY;

                let (mut write, mut read) = ws_stream.split();

                let connect_msg = ConnectMessage {
                    typ: "connect".to_string(),
                    body: ConnectBody {
                        channel: "localTimeline".to_string(),
                        id: "local".to_string(),
                        params: serde_json::json!({"withRenotes":true,"withReplies":false}),
                    },
                };

                let msg_json = serde_json::to_string(&connect_msg).unwrap();

                if let Err(e) = write.send(Message::Text(msg_json.into())).await {
                    error!("WebSocketメッセージ送信エラー: {}", e);
                    sleep(reconnect_delay).await;
                    continue;
                }

                info!("Misskey LTL購読開始");

                let ping_msg = PingMessage {
                    typ: "ping".to_string(),
                };
                let mut ping_interval = interval(Duration::from_secs(30));

                loop {
                    tokio::select! {
                        message = read.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    if let Some(note) = process_message(&text).await {
                                        if tx.send(note).await.is_err() {
                                            info!("受信チャネルが閉じられたため終了します");
                                            return;
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    if let Err(e) = write.send(Message::Pong(data)).await {
                                        error!("Pongメッセージ送信エラー: {}", e);
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(_))) => {
                                    info!("WebSocketが正常にクローズされました");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!("WebSocketメッセージ受信エラー: {}", e);
                                    break;
                                }
                                None => {
                                    info!("WebSocket接続が終了しました");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        _ = ping_interval.tick() => {
                            let ping_json = serde_json::to_string(&ping_msg).unwrap();
                            if let Err(e) = write.send(Message::Text(ping_json.into())).await {
                                error!("ハートビート送信エラー: {}", e);
                                break;
                            }
                        }
                    }
                }

                info!("WebSocket接続終了");

                if connection_start.elapsed() < Duration::from_secs(10) {
                    reconnect_attempts += 1;

                    if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                        error!(
                            "最大再接続試行回数（{}回）に達しました。一時停止します",
                            MAX_RECONNECT_ATTEMPTS
                        );
                        sleep(Duration::from_secs(60)).await;
                        reconnect_attempts = 0;
                    } else {
                        reconnect_delay = std::cmp::min(reconnect_delay * 2, MAX_RECONNECT_DELAY);

                        warn!(
                            "接続が短時間で切断されました。{}秒後に再接続を試みます（試行 {}/{}）",
                            reconnect_delay.as_secs(),
                            reconnect_attempts,
                            MAX_RECONNECT_ATTEMPTS
                        );
                        sleep(reconnect_delay).await;
                    }
                } else {
                    info!("接続が切断されました。すぐに再接続します");
                    sleep(Duration::from_millis(100)).await;
                }
            }
            Err(e) => {
                error!("WebSocket接続エラー: {}", e);

                reconnect_attempts += 1;
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    error!(
                        "最大再接続試行回数（{}回）に達しました。一時停止します",
                        MAX_RECONNECT_ATTEMPTS
                    );
                    sleep(Duration::from_secs(60)).await;
                    reconnect_attempts = 0;
                } else {
                    warn!(
                        "{}秒後に再接続を試みます（試行 {}/{}）",
                        reconnect_delay.as_secs(),
                        reconnect_attempts,
                        MAX_RECONNECT_ATTEMPTS
                    );
                    sleep(reconnect_delay).await;
                    reconnect_delay = std::cmp::min(reconnect_delay * 2, MAX_RECONNECT_DELAY);
                }
            }
        }
    }
}

async fn process_message(text: &str) -> Option<MisskeyNote> {
    let json: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return None,
    };

    if json["type"] == "channel" {
        if let Some(body) = json["body"].as_object() {
            if body["type"] == "note" {
                if let Ok(note) = serde_json::from_value::<MisskeyNote>(body["body"].clone()) {
                    if let Some(text) = &note.text {
                        if !text.trim().is_empty() {
                            return Some(note);
                        }
                    }
                }
            }
        }
    }

    None
}
