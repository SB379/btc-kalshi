use crate::normalize::local_ts_ms;
use shared_types::{Exchange, Trade};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn, error};
use futures_util::{SinkExt, StreamExt};

const WS_URL: &str = "wss://secure.lmaxdigital.com/marketdata";
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// LMAX Digital WS trade message:
/// {"type":"TRADE","price":"69800.00","qty":"0.001","timestamp":1234567890123}
pub fn normalize(raw: &str) -> Result<Vec<Trade>, serde_json::Error> {
    let v: serde_json::Value = serde_json::from_str(raw)?;
    if v.get("type").and_then(|t| t.as_str()) != Some("TRADE") {
        return Ok(vec![]);
    }
    let local = local_ts_ms();
    let price = match v["price"].as_str().and_then(|s| s.parse::<f64>().ok()) {
        Some(p) => p,
        None => return Ok(vec![]),
    };
    let size = v["qty"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let exchange_ts = v["timestamp"].as_u64().unwrap_or(local);
    Ok(vec![Trade {
        exchange: Exchange::Lmax,
        price,
        size,
        exchange_ts,
        local_ts: local,
    }])
}

pub async fn run(tx: mpsc::Sender<Trade>) {
    let mut backoff_ms = 100u64;
    loop {
        match connect_and_consume(&tx).await {
            Ok(()) => warn!(exchange = "lmax", "WebSocket closed cleanly, reconnecting"),
            Err(e) => error!(exchange = "lmax", error = %e, "WebSocket error, reconnecting"),
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
        "type": "SUBSCRIBE",
        "channels": ["TRADE"],
        "productIds": ["BTC-USD"]
    });
    sink.send(Message::Text(subscribe.to_string())).await?;
    info!(exchange = "lmax", "subscribed to BTC-USD TRADE");

    loop {
        match timeout(HEARTBEAT_TIMEOUT, stream.next()).await {
            Err(_) => {
                warn!(exchange = "lmax", "no message in 5s");
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
                                    warn!(exchange = "lmax", "merge channel full, dropping trade");
                                }
                            }
                        }
                        Err(e) => {
                            warn!(exchange = "lmax", error = %e, "failed to parse message");
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
    fn parses_lmax_trade_message() {
        let raw = r#"{"type":"TRADE","price":"69800.00","qty":"0.001","timestamp":1712534400000}"#;
        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.exchange, Exchange::Lmax);
        assert!((t.price - 69800.0).abs() < f64::EPSILON);
        assert!((t.size - 0.001).abs() < 1e-9);
        assert_eq!(t.exchange_ts, 1_712_534_400_000);
        assert!(t.local_ts > 0);
    }

    #[test]
    fn silently_skips_non_trade_messages() {
        let raw = r#"{"type":"SUBSCRIBED","channels":["TRADE"]}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }

    #[test]
    fn handles_missing_qty_gracefully() {
        // qty missing → size defaults to 0.0, trade still emitted with valid price
        let raw = r#"{"type":"TRADE","price":"69800.00","timestamp":1712534400000}"#;
        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 1);
        assert!((trades[0].size - 0.0).abs() < f64::EPSILON);
    }
}
