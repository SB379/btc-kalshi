use anyhow::Context;
use reqwest::Client;
use shared_types::TradeSide;
use tracing::{debug, info};

use crate::auth::KalshiAuth;

pub struct KalshiClient {
    pub http: Client,
    pub auth: KalshiAuth,
    pub base_url: String,
    pub use_demo: bool,
}

pub struct OrderResponse {
    pub order_id: String,
    /// "resting", "filled", "cancelled", etc.
    pub status: String,
    pub filled_count: u64,
    pub remaining_count: u64,
}

pub struct Position {
    pub ticker: String,
    pub side: TradeSide,
    pub contracts: u64,
    pub entry_price_cents: u64,
}

impl KalshiClient {
    /// Construct from environment variables.
    /// Hard-fails if `KALSHI_USE_DEMO` is not set — the executor refuses to start without it.
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let use_demo_str = std::env::var("KALSHI_USE_DEMO").map_err(|_| {
            anyhow::anyhow!(
                "KALSHI_USE_DEMO env var is required but not set — refusing to start executor. \
                 Set KALSHI_USE_DEMO=true for demo trading or KALSHI_USE_DEMO=false for live."
            )
        })?;
        let use_demo = use_demo_str.trim().to_lowercase() == "true";

        let base_url = std::env::var("KALSHI_BASE_URL")
            .unwrap_or_else(|_| "https://api.elections.kalshi.com/trade-api/v2".to_string());

        let auth = KalshiAuth::from_env().context("failed to initialize Kalshi auth")?;

        Ok(KalshiClient { http: Client::new(), auth, base_url, use_demo })
    }

    /// Fetch the account's available balance in cents.
    pub async fn get_balance(&self) -> Result<u64, anyhow::Error> {
        let path = "/portfolio/balance";
        let headers = self.auth.sign_request("GET", path)?;
        let url = format!("{}{}", self.base_url, path);

        let key_id = headers
            .get("kalshi-access-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        debug!(url = %url, kalshi_key_id = %key_id, signature = "<redacted>", "GET /portfolio/balance");

        let response = self
            .http
            .get(&url)
            .headers(headers)
            .send()
            .await
            .context("GET /portfolio/balance request failed")?;
        let text = response.text().await.context("failed to read balance response body")?;
        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse balance response");
            anyhow::anyhow!("failed to parse balance response: {}", e)
        })?;

        debug!(response = %body, "balance response");

        // Kalshi returns available_balance_cents; fall back to balance for compatibility
        let cents = body
            .get("available_balance_cents")
            .or_else(|| body.get("balance"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok(cents)
    }

    /// Place a limit order on Kalshi.
    /// Logs the full request and response at DEBUG, and a summary at INFO on success.
    pub async fn place_order(
        &self,
        ticker: &str,
        side: &TradeSide,
        contracts: u64,
        limit_price_cents: u64,
    ) -> Result<OrderResponse, anyhow::Error> {
        let path = "/portfolio/orders";
        let headers = self.auth.sign_request("POST", path)?;
        let url = format!("{}{}", self.base_url, path);

        let side_str = match side {
            TradeSide::Yes => "yes",
            TradeSide::No => "no",
        };

        let mut body = serde_json::json!({
            "ticker": ticker,
            "side": side_str,
            "count": contracts,
            "type": "limit",
        });
        match side {
            TradeSide::Yes => body["yes_price"] = serde_json::Value::from(limit_price_cents),
            TradeSide::No => body["no_price"] = serde_json::Value::from(limit_price_cents),
        }

        let key_id = headers
            .get("kalshi-access-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        debug!(
            url = %url,
            kalshi_key_id = %key_id,
            signature = "<redacted>",
            ticker = ticker,
            side = side_str,
            contracts = contracts,
            limit_cents = limit_price_cents,
            "POST /portfolio/orders"
        );

        let response = self
            .http
            .post(&url)
            .headers(headers)
            .json(&body)
            .send()
            .await
            .context("POST /portfolio/orders request failed")?;
        let text = response.text().await.context("failed to read order response body")?;
        let resp: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse order response");
            anyhow::anyhow!("failed to parse order response: {}", e)
        })?;

        debug!(response = %resp, "order response");

        // Kalshi wraps the order in an "order" key
        let order = resp.get("order").unwrap_or(&resp);

        let order_id = order
            .get("order_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let status = order
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let filled_count = order
            .get("fill_count")
            .or_else(|| order.get("filled_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let remaining_count = order
            .get("remaining_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(contracts.saturating_sub(filled_count));

        info!(
            order_id = %order_id,
            status = %status,
            filled = filled_count,
            remaining = remaining_count,
            ticker = ticker,
            "order placed"
        );

        Ok(OrderResponse { order_id, status, filled_count, remaining_count })
    }

    /// Fetch all unsettled (open) positions.
    pub async fn get_open_positions(&self) -> Result<Vec<Position>, anyhow::Error> {
        let path = "/portfolio/positions?settlement_status=unsettled";
        let headers = self.auth.sign_request("GET", path)?;
        let url = format!("{}{}", self.base_url, path);

        let key_id = headers
            .get("kalshi-access-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        debug!(url = %url, kalshi_key_id = %key_id, signature = "<redacted>", "GET /portfolio/positions");

        let response = self
            .http
            .get(&url)
            .headers(headers)
            .send()
            .await
            .context("GET /portfolio/positions request failed")?;
        let text = response.text().await.context("failed to read positions response body")?;
        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse positions response");
            anyhow::anyhow!("failed to parse positions response: {}", e)
        })?;

        debug!(response = %body, "positions response");

        let positions_arr = match body.get("market_positions").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return Ok(vec![]),
        };

        let mut result = Vec::new();
        for p in positions_arr {
            let ticker = match p.get("ticker").and_then(|v| v.as_str()) {
                Some(t) => t.to_string(),
                None => continue,
            };
            // Positive position = YES contracts, negative = NO contracts
            let raw_position = p.get("position").and_then(|v| v.as_i64()).unwrap_or(0);
            if raw_position == 0 {
                continue;
            }
            let (side, contracts) = if raw_position > 0 {
                (TradeSide::Yes, raw_position as u64)
            } else {
                (TradeSide::No, raw_position.unsigned_abs())
            };
            let entry_price_cents = p
                .get("average_yes_price")
                .or_else(|| p.get("average_no_price"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            result.push(Position { ticker, side, contracts, entry_price_cents });
        }
        Ok(result)
    }
}
