use crate::normalize::local_ts_ms;
use serde::Deserialize;
use shared_types::{Exchange, Trade};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn, error};
use futures_util::{SinkExt, StreamExt};

const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Actual Coinbase Advanced Trade WS shape:
/// {"channel":"market_trades","events":[{"type":"update","trades":[{"price":"69800.00","size":"0.001","time":"..."}]}]}
///
/// The top-level discriminant is `channel`, not `type`. Non-trade messages (subscriptions,
/// heartbeats) have a different or absent `channel` value and are silently skipped.
#[derive(Deserialize, Debug)]
struct CoinbaseMsg {
    // Option so messages without `channel` (e.g. subscription acks) parse and skip cleanly
    channel: Option<String>,
    events: Option<Vec<CoinbaseEvent>>,
}

#[derive(Deserialize, Debug)]
struct CoinbaseEvent {
    trades: Option<Vec<CoinbaseRawTrade>>,
}

#[derive(Deserialize, Debug)]
struct CoinbaseRawTrade {
    price: String,
    size: String,
    time: String,
}

/// Parse a single Coinbase WS message into zero or more Trades.
pub fn normalize(raw: &str) -> Result<Vec<Trade>, serde_json::Error> {
    let msg: CoinbaseMsg = serde_json::from_str(raw)?;
    if msg.channel.as_deref() != Some("market_trades") {
        return Ok(vec![]);
    }
    let local = local_ts_ms();
    let mut trades = Vec::new();
    for event in msg.events.unwrap_or_default() {
        for t in event.trades.unwrap_or_default() {
            let price: f64 = match t.price.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let size: f64 = match t.size.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let exchange_ts = parse_rfc3339_ms(&t.time).unwrap_or(local);
            trades.push(Trade {
                exchange: Exchange::Coinbase,
                price,
                size,
                exchange_ts,
                local_ts: local,
            });
        }
    }
    Ok(trades)
}

fn parse_rfc3339_ms(s: &str) -> Option<u64> {
    let dt = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()?;
    Some((dt.unix_timestamp() as u64) * 1000 + (dt.nanosecond() as u64) / 1_000_000)
}

pub async fn run(tx: mpsc::Sender<Trade>) {
    let mut backoff_ms = 100u64;
    loop {
        match connect_and_consume(&tx).await {
            Ok(()) => warn!(exchange = "coinbase", "WebSocket closed cleanly, reconnecting"),
            Err(e) => error!(exchange = "coinbase", error = %e, "WebSocket error, reconnecting"),
        }
        let jitter = jitter(backoff_ms);
        tokio::time::sleep(Duration::from_millis(jitter)).await;
        backoff_ms = (backoff_ms * 2).min(30_000);
    }
}

async fn connect_and_consume(tx: &mpsc::Sender<Trade>) -> Result<(), Box<dyn std::error::Error>> {
    let (ws_stream, _) = connect_async(WS_URL).await?;
    let (mut sink, mut stream) = ws_stream.split();

    let subscribe = serde_json::json!({
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channel": "market_trades"
    });
    sink.send(Message::Text(subscribe.to_string())).await?;
    info!(exchange = "coinbase", "subscribed to BTC-USD market_trades");

    loop {
        match timeout(HEARTBEAT_TIMEOUT, stream.next()).await {
            Err(_) => {
                warn!(exchange = "coinbase", "no message in 5s");
                continue;
            }
            Ok(None) => return Ok(()),
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(Some(Ok(msg))) => {
                if let Message::Text(text) = msg {
                    match normalize(&text) {
                        Ok(trades) => {
                            for trade in trades {
                                if tx.try_send(trade).is_err() {
                                    warn!(exchange = "coinbase", "merge channel full, dropping trade");
                                }
                            }
                        }
                        Err(e) => {
                            warn!(exchange = "coinbase", error = %e, "failed to parse message");
                        }
                    }
                }
            }
        }
    }
}

fn jitter(base: u64) -> u64 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let factor = rng.gen_range(0.8..=1.2);
    (base as f64 * factor) as u64
}

#[cfg(test)]
mod tests {
    use super::normalize;
    use shared_types::Exchange;

    #[test]
    fn parses_coinbase_trade_message() {
        let raw = r#"{
            "channel": "market_trades",
            "events": [
                {
                    "type": "update",
                    "trades": [
                        {
                            "trade_id": "...",
                            "product_id": "BTC-USD",
                            "price": "69800.00",
                            "size": "0.001",
                            "side": "BUY",
                            "time": "2026-04-06T21:00:00.000000Z"
                        }
                    ]
                }
            ]
        }"#;

        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.exchange, Exchange::Coinbase);
        assert!((t.price - 69800.0).abs() < f64::EPSILON);
        assert!((t.size - 0.001).abs() < 1e-9);
        assert!(t.exchange_ts > 0);
        assert!(t.local_ts > 0);
    }

    #[test]
    fn multiple_trades_in_one_message() {
        let raw = r#"{
            "channel": "market_trades",
            "events": [
                {
                    "type": "update",
                    "trades": [
                        {"trade_id": "1", "product_id": "BTC-USD", "price": "69800.00", "size": "0.001", "side": "BUY", "time": "2026-04-06T21:00:00.000000Z"},
                        {"trade_id": "2", "product_id": "BTC-USD", "price": "69801.00", "size": "0.002", "side": "SELL", "time": "2026-04-06T21:00:00.001000Z"}
                    ]
                }
            ]
        }"#;

        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 2);
    }

    #[test]
    fn silently_skips_non_market_trades_messages() {
        // Subscription ack — no `channel` field at all
        let raw = r#"{"type":"subscriptions","channels":[]}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }
}
