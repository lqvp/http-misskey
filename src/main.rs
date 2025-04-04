mod connection_counter;
mod html;
mod misskey;
mod model;

use axum::{routing::get, Router};
use clap::Parser;
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use time::macros::format_description;
use tokio::sync::{broadcast, watch};
use tokio::time::{interval, Duration};
use tracing::info;
use tracing_subscriber::fmt::time::OffsetTime;

use crate::misskey::MisskeyNote;
use crate::model::{Context, MisskeyData};
use connection_counter::ConnectionCounter;

type Clock = watch::Receiver<MisskeyData>;

static BROADCAST_CHANNEL: Lazy<(
    broadcast::Sender<MisskeyNote>,
    Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
)> = Lazy::new(|| {
    let (tx, _) = broadcast::channel(100);
    let tx_clone = tx.clone();

    let handle = Arc::new(tokio::sync::Mutex::new(None));
    let handle_clone = handle.clone();

    tokio::spawn(async move {
        let handle = misskey::start_misskey_ltl_broadcast(tx_clone).await;
        let mut lock = handle_clone.lock().await;
        *lock = Some(handle);
    });

    (tx, handle)
});

static COUNTER_ID: AtomicUsize = AtomicUsize::new(1);

fn encode_data(previous_id: usize, counter: &ConnectionCounter) -> (usize, MisskeyData) {
    let current_id = COUNTER_ID.fetch_add(1, Ordering::SeqCst);

    let ctx = Context {
        previous_id,
        current_id,
        connection_count: counter.current(),
    };

    (
        current_id,
        MisskeyData {
            html: html::encode_connection_count(&ctx),
        },
    )
}

#[derive(Debug, Parser)]
struct Cli {
    #[clap(long, env)]
    #[clap(default_value = "0.0.0.0:3000")]
    listen: SocketAddr,

    #[clap(long, env)]
    misskey_token: Option<String>,
}

#[tokio::main]
async fn main() {
    let timer = OffsetTime::new(
        time::UtcOffset::from_hms(9, 0, 0).unwrap(),
        format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]"),
    );

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_timer(timer)
        .init();

    let cli = Cli::parse();

    let connection_counter = ConnectionCounter::new();

    let _ = &BROADCAST_CHANNEL;
    info!("ブロードキャストチャネルを初期化しました");

    let (clock_source, clock) = watch::channel(encode_data(0, &connection_counter).1);

    let counter_clone = connection_counter.clone();
    tokio::spawn(async move {
        let mut previous_id: usize = 0;
        let mut interval = interval(Duration::from_secs(5));

        loop {
            interval.tick().await;

            let data = encode_data(previous_id, &counter_clone);

            if clock_source.send(data.1).is_ok() {
                previous_id = data.0;
            }
        }
    });

    let app = Router::new()
        .route("/", get(html::handler))
        .with_state((clock, connection_counter));

    info!("サーバーを開始します: http://{}", cli.listen);

    let listener = tokio::net::TcpListener::bind(&cli.listen).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
