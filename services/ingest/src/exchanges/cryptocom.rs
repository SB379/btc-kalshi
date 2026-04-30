use crate::normalize::local_ts_ms;
use futures_util::{SinkExt, StreamExt};
use shared_types::{Exchange, Trade};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const WS_URL: &str = "wss://stream.crypto.com/exchange/v1/market";
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Crypto.com WS trade update:
/// {"id":-1,"method":"subscribe","result":{"channel":"trade.BTC_USD","data":[{"p":"69800.00","q":"0.001","t":1712534400000,...}]}}
pub fn normalize(raw: &str) -> Result<Vec<Trade>, serde_json::Error> {
    let v: serde_json::Value = serde_json::from_str(raw)?;

    // Only handle "subscribe" method responses with trade data
    if v.get("method").and_then(|m| m.as_str()) != Some("subscribe") {
        return Ok(vec![]);
    }

    let data = match v
        .get("result")
        .and_then(|r| r.get("data"))
        .and_then(|d| d.as_array())
    {
        Some(arr) => arr,
        None => return Ok(vec![]),
    };

    let local = local_ts_ms();
    let mut trades = Vec::new();
    for entry in data {
        let price = match entry["p"].as_str().and_then(|s| s.parse::<f64>().ok()) {
            Some(p) => p,
            None => continue,
        };
        let size = entry["q"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let exchange_ts = entry["t"].as_u64().unwrap_or(local);
        trades.push(Trade {
            exchange: Exchange::CryptoCom,
            price,
            size,
            exchange_ts,
            local_ts: local,
        });
    }
    Ok(trades)
}

pub async fn run(tx: mpsc::Sender<Trade>) {
    let mut backoff_ms = 100u64;
    loop {
        match connect_and_consume(&tx).await {
            Ok(()) => warn!(
                exchange = "crypto.com",
                "WebSocket closed cleanly, reconnecting"
            ),
            Err(e) => error!(exchange = "crypto.com", error = %e, "WebSocket error, reconnecting"),
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
        "id": 1,
        "method": "subscribe",
        "params": {
            "channels": ["trade.BTC_USD"]
        }
    });
    sink.send(Message::Text(subscribe.to_string())).await?;
    info!(exchange = "crypto.com", "subscribed to trade.BTC_USD");

    loop {
        match timeout(HEARTBEAT_TIMEOUT, stream.next()).await {
            Err(_) => {
                warn!(exchange = "crypto.com", "no message in 5s");
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
                                    warn!(
                                        exchange = "crypto.com",
                                        "merge channel full, dropping trade"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(exchange = "crypto.com", error = %e, "failed to parse message");
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
    fn parses_cryptocom_trade_message() {
        let raw = r#"{
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "trade.BTC_USD",
                "data": [
                    {"p": "69800.00", "q": "0.001", "t": 1712534400000, "s": "BUY", "d": 123}
                ]
            }
        }"#;
        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.exchange, Exchange::CryptoCom);
        assert!((t.price - 69800.0).abs() < f64::EPSILON);
        assert!((t.size - 0.001).abs() < 1e-9);
        assert_eq!(t.exchange_ts, 1_712_534_400_000);
        assert!(t.local_ts > 0);
    }

    #[test]
    fn multiple_trades_in_one_message() {
        let raw = r#"{
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "trade.BTC_USD",
                "data": [
                    {"p": "69800.00", "q": "0.001", "t": 1712534400000, "s": "BUY", "d": 1},
                    {"p": "69801.00", "q": "0.002", "t": 1712534400001, "s": "SELL", "d": 2}
                ]
            }
        }"#;
        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 2);
    }

    #[test]
    fn silently_skips_non_subscribe_messages() {
        // Subscription ack or heartbeat
        let raw = r#"{"id":1,"method":"subscribe","code":0}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }

    #[test]
    fn silently_skips_missing_result_data() {
        let raw = r#"{"id":-1,"method":"subscribe","result":{"channel":"trade.BTC_USD"}}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }

    #[test]
    fn skips_entry_with_missing_price() {
        let raw = r#"{
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "trade.BTC_USD",
                "data": [{"q": "0.001", "t": 1712534400000}]
            }
        }"#;
        let trades = normalize(raw).expect("should parse");
        assert!(trades.is_empty());
    }
}
