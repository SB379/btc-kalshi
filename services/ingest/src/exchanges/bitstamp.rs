use crate::normalize::local_ts_ms;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use shared_types::{Exchange, Trade};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const WS_URL: &str = "wss://ws.bitstamp.net";
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Bitstamp WS live_trades_btcusd event.
/// {"event":"trade","channel":"live_trades_btcusd","data":{"id":123,"timestamp":"1705319696","microtimestamp":"1705319696789000","amount":0.12345,"amount_str":"0.12345","price":43210.5,"price_str":"43210.5","type":0,"buy_order_id":1,"sell_order_id":2}}
///
/// Non-trade events (e.g. bts:subscription_succeeded) have a different or empty data shape,
/// so we check the event type before attempting to deserialize data fields.
#[derive(Deserialize, Debug)]
struct BitstampTradeMsg {
    #[allow(dead_code)]
    event: String,
    data: BitstampData,
}

#[derive(Deserialize, Debug)]
struct BitstampData {
    microtimestamp: String,
    #[serde(rename = "amount_str")]
    amount: String,
    #[serde(rename = "price_str")]
    price: String,
}

#[derive(Deserialize, Debug)]
struct BitstampEventOnly {
    event: String,
}

pub fn normalize(raw: &str) -> Result<Vec<Trade>, serde_json::Error> {
    // Check event type cheaply before deserializing the full data payload
    let envelope: BitstampEventOnly = serde_json::from_str(raw)?;
    if envelope.event != "trade" {
        return Ok(vec![]);
    }
    let msg: BitstampTradeMsg = serde_json::from_str(raw)?;
    let local = local_ts_ms();
    let price: f64 = match msg.data.price.parse() {
        Ok(v) => v,
        Err(_) => return Ok(vec![]),
    };
    let size: f64 = match msg.data.amount.parse() {
        Ok(v) => v,
        Err(_) => return Ok(vec![]),
    };
    // microtimestamp is microseconds since epoch as a decimal string
    let exchange_ts = msg
        .data
        .microtimestamp
        .parse::<u64>()
        .map(|us| us / 1000)
        .unwrap_or(local);

    Ok(vec![Trade {
        exchange: Exchange::Bitstamp,
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
            Ok(()) => warn!(
                exchange = "bitstamp",
                "WebSocket closed cleanly, reconnecting"
            ),
            Err(e) => error!(exchange = "bitstamp", error = %e, "WebSocket error, reconnecting"),
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
        "event": "bts:subscribe",
        "data": {
            "channel": "live_trades_btcusd"
        }
    });
    sink.send(Message::Text(subscribe.to_string())).await?;
    info!(exchange = "bitstamp", "subscribed to live_trades_btcusd");

    loop {
        match timeout(HEARTBEAT_TIMEOUT, stream.next()).await {
            Err(_) => {
                warn!(exchange = "bitstamp", "no message in 5s");
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
                                        exchange = "bitstamp",
                                        "merge channel full, dropping trade"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(exchange = "bitstamp", error = %e, "failed to parse message");
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
    fn parses_bitstamp_trade_message() {
        // Real Bitstamp WS live_trades format
        let raw = r#"{
            "event": "trade",
            "channel": "live_trades_btcusd",
            "data": {
                "id": 123456789,
                "timestamp": "1705319696",
                "microtimestamp": "1705319696789000",
                "amount": 0.12345,
                "amount_str": "0.12345000",
                "price": 43210.5,
                "price_str": "43210.50",
                "type": 0,
                "buy_order_id": 100,
                "sell_order_id": 200
            }
        }"#;

        let trades = normalize(raw).expect("should parse");
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.exchange, Exchange::Bitstamp);
        assert!((t.price - 43210.5).abs() < f64::EPSILON);
        assert!((t.size - 0.12345).abs() < 1e-9);
        // microtimestamp 1705319696789000 us → 1705319696789 ms
        assert_eq!(t.exchange_ts, 1_705_319_696_789);
        assert!(t.local_ts > 0);
    }

    #[test]
    fn ignores_non_trade_events() {
        let raw =
            r#"{"event":"bts:subscription_succeeded","channel":"live_trades_btcusd","data":{}}"#;
        let trades = normalize(raw).expect("should parse");
        assert!(trades.is_empty());
    }
}
