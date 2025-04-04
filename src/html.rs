use crate::misskey::MisskeyNote;
use crate::model::Context;
use crate::{Clock, ConnectionCounter, BROADCAST_CHANNEL};

use async_stream::try_stream;
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, HeaderName},
    response::IntoResponse,
};
use bytes::Bytes;
use futures::Stream;
use tokio::time::{sleep, Duration};
use tracing::info;

fn escape_html(text: &str) -> String {
    text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br>")
}

fn encode_note(note: &MisskeyNote) -> Bytes {
    let text = note.text.as_deref().unwrap_or("").to_string();
    let text = escape_html(&text);

    let created_at = chrono::DateTime::parse_from_rfc3339(&note.created_at)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|_| chrono::Utc::now())
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();

    Bytes::from(format!(
        r#"<div class="note">
  <div class="article">
    <div class="avatar">
      <img src="{}" alt="avatar" class="avatar-img">
    </div>
    <div class="main">
      <div class="note-header">
        <span class="user-name">{}</span>
        <span class="user-username">@{}</span>
        <span class="note-time">{}</span>
      </div>
      <div class="note-content">{}</div>
    </div>
  </div>
</div>
"#,
        note.user.avatar_url, note.user.name, note.user.username, created_at, text
    ))
}

pub fn encode_connection_count(ctx: &Context) -> Bytes {
    let user_emojis = if ctx.connection_count <= 50 {
        "👤".repeat(ctx.connection_count)
    } else {
        format!("👤 x {}", ctx.connection_count)
    };

    let counter_id = format!("counter-{}", ctx.current_id);
    let prev_counter_id = format!("counter-{}", ctx.previous_id);

    let mut html = String::from("<style>");
    if ctx.previous_id > 0 {
        html.push_str(&format!("#{} {{ display: none; }}\n", prev_counter_id));
    }
    html.push_str("</style>\n");

    html.push_str(&format!(
        r#"<div id="{}" class="connection-counter">接続中: <strong>{}</strong>人 <span class="user-icons">{}</span></div>"#,
        counter_id, ctx.connection_count, user_emojis
    ));

    Bytes::from(html)
}

fn stream(
    clock: Clock,
    counter: ConnectionCounter,
) -> impl Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>> {
    let mut rx = BROADCAST_CHANNEL.0.subscribe();

    try_stream! {
        let _session = counter.acquire();

        {
            let html = clock.borrow().html.clone();
            yield html;
        }

        yield Bytes::from_static(include_bytes!("../assets/head.html"));

        let mut clock_clone = clock.clone();

        loop {
            tokio::select! {
                result = clock_clone.changed() => {
                    if result.is_ok() {
                        let html = clock_clone.borrow().html.clone();
                        yield html;
                    }
                },

                result = rx.recv() => {
                    match result {
                        Ok(note) => {
                            info!("ノート受信: {}", note.id);
                            let html = encode_note(&note);
                            yield html;
                            sleep(Duration::from_millis(50)).await;
                        },
                        Err(e) => {
                            info!("ブロードキャスト受信エラー: {}", e);
                            sleep(Duration::from_secs(1)).await;
                            rx = BROADCAST_CHANNEL.0.subscribe();
                        }
                    }
                }
            }
        }
    }
}

pub async fn handler(
    headers: HeaderMap,
    State((clock, counter)): State<(Clock, ConnectionCounter)>,
) -> impl IntoResponse {
    info!("新規リクエスト受信");

    let stream = stream(clock, counter);
    let body = Body::from_stream(stream);

    let is_cloudflare = headers.contains_key("cf-ray");
    info!("Cloudflare経由: {}", is_cloudflare);

    let headers = [
        (
            header::CONTENT_TYPE,
            if is_cloudflare {
                "application/grpc"
            } else {
                "text/html; charset=utf-8"
            },
        ),
        (
            HeaderName::from_static("x-original-content-type"),
            "text/html; charset=utf-8",
        ),
        (
            header::CACHE_CONTROL,
            "no-store, no-cache, must-revalidate, max-age=0",
        ),
    ];

    info!("レスポンス返却開始");
    (headers, body)
}
