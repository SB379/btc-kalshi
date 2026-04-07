use crate::normalize::local_ts_ms;
use shared_types::{Exchange, Trade};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn, error};
use futures_util::{SinkExt, StreamExt};

const WS_URL: &str = "wss://ws.kraken.com/v2";
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Parse a Kraken WS v2 message into zero or more Trades.
///
/// We deserialize to Value first because:
/// - System messages sent on connect have no "channel" key at all — typed structs
///   would return a hard missing-field error instead of a silent skip.
/// - heartbeat / status data arrays contain non-trade objects whose schema differs
///   from trade entries; gating on channel/type before touching `data` is safest.
pub fn normalize(raw: &str) -> Result<Vec<Trade>, serde_json::Error> {
    let v: serde_json::Value = serde_json::from_str(raw)?;

    // System messages (e.g. initial connection ack) have no "channel" key — skip silently
    let channel = match v.get("channel").and_then(|c| c.as_str()) {
        Some(c) => c,
        None => return Ok(vec![]),
    };

    if channel == "heartbeat" || channel == "status" {
        return Ok(vec![]);
    }

    let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if channel != "trade" || msg_type != "update" {
        return Ok(vec![]);
    }

    let local = local_ts_ms();
    let data = match v.get("data").and_then(|d| d.as_array()) {
        Some(arr) => arr,
        None => return Ok(vec![]),
    };

    let mut trades = Vec::new();
    for entry in data {
        let price = match entry.get("price").and_then(|p| p.as_f64()) {
            Some(p) => p,
            None => continue,
        };
        let qty = match entry.get("qty").and_then(|q| q.as_f64()) {
            Some(q) => q,
            None => continue,
        };
        let timestamp = match entry.get("timestamp").and_then(|t| t.as_str()) {
            Some(ts) => ts,
            None => continue,
        };
        let exchange_ts = parse_rfc3339_ms(timestamp).unwrap_or(local);
        trades.push(Trade {
            exchange: Exchange::Kraken,
            price,
            size: qty,
            exchange_ts,
            local_ts: local,
        });
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
            Ok(()) => warn!(exchange = "kraken", "WebSocket closed cleanly, reconnecting"),
            Err(e) => error!(exchange = "kraken", error = %e, "WebSocket error, reconnecting"),
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
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": ["BTC/USD"]
        }
    });
    sink.send(Message::Text(subscribe.to_string())).await?;
    info!(exchange = "kraken", "subscribed to XBT/USD trade");

    loop {
        match timeout(HEARTBEAT_TIMEOUT, stream.next()).await {
            Err(_) => {
                warn!(exchange = "kraken", "no message in 5s");
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
                                    warn!(exchange = "kraken", "merge channel full, dropping trade");
                                }
                            }
                        }
                        Err(e) => {
                            warn!(exchange = "kraken", error = %e, "failed to parse message");
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
    fn parses_kraken_trade_message() {
        let raw = r#"{
            "channel": "trade",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "price": 69800.12,
                    "qty": 0.001,
                    "timestamp": "2026-04-06T21:00:00.000000Z",
                    "side": "buy",
                    "ord_type": "market"
                }
            ]
        }"#;

        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.exchange, Exchange::Kraken);
        assert!((t.price - 69800.12).abs() < f64::EPSILON);
        assert!((t.size - 0.001).abs() < 1e-9);
        assert!(t.exchange_ts > 0);
        assert!(t.local_ts > 0);
    }

    #[test]
    fn silently_skips_no_channel_system_message() {
        // Kraken sends these on connect — no "channel" key at all
        let raw = r#"{"method":"subscribe","result":{"channel":"trade","symbol":"XBT/USD"},"success":true,"time_in":"2026-04-06T21:00:00.000000Z","time_out":"2026-04-06T21:00:00.001000Z"}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }

    #[test]
    fn silently_skips_heartbeat() {
        let raw = r#"{"channel":"heartbeat","type":"heartbeat"}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }

    #[test]
    fn silently_skips_status() {
        // Status data contains non-trade objects with completely different fields
        let raw = r#"{"channel":"status","type":"update","data":[{"api_version":"2.0.0","connection_id":"abc","system":"online","version":"2.0.0"}]}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }

    #[test]
    fn silently_skips_snapshot() {
        let raw = r#"{"channel":"trade","type":"snapshot","data":[]}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }
}
