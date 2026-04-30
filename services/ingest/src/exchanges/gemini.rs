use crate::normalize::local_ts_ms;
use futures_util::StreamExt;
use shared_types::{Exchange, Trade};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const WS_URL: &str = "wss://api.gemini.com/v1/marketdata/BTCUSD";
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Parse a Gemini market data WS message into zero or more Trades.
///
/// We deserialize to Value first because:
/// - `#[serde(other)]` in internally-tagged serde enums has edge cases where unexpected
///   fields in sibling event types (change, auction, etc.) can silently abort iteration,
///   yielding zero trades even when valid trade entries are present.
/// - Value-based parsing iterates the events array entry-by-entry and skips only the
///   individual entries that aren't type:"trade", never aborting the whole message.
///
/// Message shape:
///   {"type":"update","eventId":123,"timestampms":1775509255000,
///    "events":[{"type":"trade","tid":456,"price":"69800.00","amount":"0.001","makerSide":"bid"}]}
/// exchange_ts = top-level timestampms (not per-event).
pub fn normalize(raw: &str) -> Result<Vec<Trade>, serde_json::Error> {
    let v: serde_json::Value = serde_json::from_str(raw)?;

    if v.get("type").and_then(|t| t.as_str()) == Some("heartbeat") {
        return Ok(vec![]);
    }

    let local = local_ts_ms();
    // exchange_ts is on the outer message envelope, shared by all trades in this batch
    let exchange_ts = v
        .get("timestampms")
        .and_then(|t| t.as_u64())
        .unwrap_or(local);

    let events = match v.get("events").and_then(|e| e.as_array()) {
        Some(arr) => arr,
        None => return Ok(vec![]),
    };

    let mut trades = Vec::new();
    for event in events {
        if event.get("type").and_then(|t| t.as_str()) != Some("trade") {
            continue;
        }
        let price_str = match event.get("price").and_then(|p| p.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let amount_str = match event.get("amount").and_then(|a| a.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let price: f64 = match price_str.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let size: f64 = match amount_str.parse() {
            Ok(s) => s,
            Err(_) => continue,
        };
        trades.push(Trade {
            exchange: Exchange::Gemini,
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
                exchange = "gemini",
                "WebSocket closed cleanly, reconnecting"
            ),
            Err(e) => error!(exchange = "gemini", error = %e, "WebSocket error, reconnecting"),
        }
        let jitter = jitter(backoff_ms);
        tokio::time::sleep(Duration::from_millis(jitter)).await;
        backoff_ms = (backoff_ms * 2).min(30_000);
    }
}

async fn connect_and_consume(tx: &mpsc::Sender<Trade>) -> Result<(), Box<dyn std::error::Error>> {
    // Gemini streams automatically on connect — no subscribe message needed
    let (ws_stream, _) = connect_async(WS_URL).await?;
    let (_, mut stream) = ws_stream.split();
    info!(
        exchange = "gemini",
        "connected to BTCUSD market data stream"
    );

    loop {
        match timeout(HEARTBEAT_TIMEOUT, stream.next()).await {
            Err(_) => {
                warn!(exchange = "gemini", "no message in 5s");
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
                                        exchange = "gemini",
                                        "merge channel full, dropping trade"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(exchange = "gemini", error = %e, "failed to parse message");
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
    fn parses_gemini_trade_message() {
        let raw = r#"{
            "type": "update",
            "eventId": 123,
            "timestampms": 1775509255000,
            "events": [
                {
                    "type": "trade",
                    "tid": 456,
                    "price": "69800.00",
                    "amount": "0.001",
                    "makerSide": "bid"
                }
            ]
        }"#;

        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.exchange, Exchange::Gemini);
        assert!((t.price - 69800.0).abs() < f64::EPSILON);
        assert!((t.size - 0.001).abs() < 1e-9);
        assert_eq!(t.exchange_ts, 1_775_509_255_000);
        assert!(t.local_ts > 0);
    }

    #[test]
    fn skips_non_trade_events_individually() {
        // Each non-trade entry is skipped; the trade entry is still emitted.
        // This is the key regression test: the old serde enum approach could abort
        // the whole events array when a change event had unexpected fields.
        let raw = r#"{
            "type": "update",
            "eventId": 124,
            "timestampms": 1775509255001,
            "events": [
                {"type": "change", "side": "bid", "price": "69799.00", "remaining": "1.5", "delta": "0.5", "reason": "place"},
                {"type": "trade", "tid": 457, "price": "69800.00", "amount": "0.002", "makerSide": "ask"},
                {"type": "change", "side": "ask", "price": "69801.00", "remaining": "0.5", "delta": "-0.5", "reason": "cancel"}
            ]
        }"#;

        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 1);
        assert!((trades[0].size - 0.002).abs() < 1e-9);
    }

    #[test]
    fn silently_skips_heartbeat() {
        let raw = r#"{"type":"heartbeat","timestampms":1775509255000,"sequence":1,"trace_id":"abc","socket_sequence":1}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }

    #[test]
    fn silently_skips_update_with_no_trades() {
        let raw = r#"{"type":"update","eventId":125,"timestampms":1775509255002,"events":[]}"#;
        let trades = normalize(raw).expect("should parse without error");
        assert!(trades.is_empty());
    }
}
