#![deny(clippy::unwrap_used)]

pub mod schema;
pub mod writer;

pub use schema::{
    BrtiRecord, CompareRecord, FillRecord, OpportunityRecord, RealizedPnlRecord,
    RiskViolationRecord, SignalRecord, TradeRecord,
};

use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::Context;
use serde::Serialize;
use tracing::error;

use writer::JsonlWriter;

/// Synchronous, append-only JSONL logger. Wired into every service via `Arc<Historian>`.
///
/// Each `record_*` call serialises the record to JSON and appends one line to
/// the appropriate daily `.jsonl` file immediately — no buffering, no background
/// task, no flush cycles. Files are named `{type}_{YYYY-MM-DD}.jsonl` in the
/// directory set by `HISTORIAN_LOG_DIR` (default: `data/logs`).
///
/// Crash safety: O_APPEND writes are atomic for lines ≤ PIPE_BUF (~4 KB).
/// A crash can corrupt at most the in-flight line; all prior lines remain intact.
///
/// Read in Python:
///   pd.read_json('data/logs/brti_2026-04-07.jsonl', lines=True)
pub struct Historian {
    brti_w: Mutex<JsonlWriter>,
    trade_w: Mutex<JsonlWriter>,
    signal_w: Mutex<JsonlWriter>,
    opp_w: Mutex<JsonlWriter>,
    fill_w: Mutex<JsonlWriter>,
    pnl_w: Mutex<JsonlWriter>,
    risk_w: Mutex<JsonlWriter>,
    compare_w: Mutex<JsonlWriter>,
}

impl Historian {
    /// Initialise the historian, creating `HISTORIAN_LOG_DIR` if needed and
    /// opening all six daily files. Returns an error if the directory or any
    /// file cannot be created.
    pub fn new() -> anyhow::Result<Self> {
        let log_dir = PathBuf::from(
            std::env::var("HISTORIAN_LOG_DIR").unwrap_or_else(|_| "data/logs".to_string()),
        );
        std::fs::create_dir_all(&log_dir)
            .with_context(|| format!("create historian log dir {log_dir:?}"))?;
        let day = today_str();
        Ok(Historian {
            brti_w: Mutex::new(JsonlWriter::new("brti", log_dir.clone(), &day)?),
            trade_w: Mutex::new(JsonlWriter::new("trades", log_dir.clone(), &day)?),
            signal_w: Mutex::new(JsonlWriter::new("signals", log_dir.clone(), &day)?),
            opp_w: Mutex::new(JsonlWriter::new("opportunities", log_dir.clone(), &day)?),
            fill_w: Mutex::new(JsonlWriter::new("fills", log_dir.clone(), &day)?),
            pnl_w: Mutex::new(JsonlWriter::new("realized_pnl", log_dir.clone(), &day)?),
            risk_w: Mutex::new(JsonlWriter::new("risk_violations", log_dir.clone(), &day)?),
            compare_w: Mutex::new(JsonlWriter::new("compare", log_dir, &day)?),
        })
    }

    pub fn record_brti(&self, r: BrtiRecord) {
        write_record(&self.brti_w, &r, "brti");
    }

    pub fn record_trade(&self, r: TradeRecord) {
        write_record(&self.trade_w, &r, "trades");
    }

    pub fn record_signal(&self, r: SignalRecord) {
        write_record(&self.signal_w, &r, "signals");
    }

    pub fn record_opportunity(&self, r: OpportunityRecord) {
        write_record(&self.opp_w, &r, "opportunities");
    }

    pub fn record_fill(&self, r: FillRecord) {
        write_record(&self.fill_w, &r, "fills");
    }

    pub fn record_realized_pnl(&self, r: RealizedPnlRecord) {
        write_record(&self.pnl_w, &r, "realized_pnl");
    }

    pub fn record_risk_violation(&self, r: RiskViolationRecord) {
        write_record(&self.risk_w, &r, "risk_violations");
    }

    /// Written only in observation mode (TRADING_ENABLED=false).
    pub fn record_compare(&self, r: CompareRecord) {
        write_record(&self.compare_w, &r, "compare");
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn write_record<T: Serialize>(writer: &Mutex<JsonlWriter>, record: &T, label: &'static str) {
    match serde_json::to_string(record) {
        Ok(line) => match writer.lock() {
            Ok(mut w) => {
                if let Err(e) = w.write_line(&today_str(), &line) {
                    error!(error = %e, label, "historian: write failed");
                }
            }
            Err(_) => error!(label, "historian: writer lock poisoned"),
        },
        Err(e) => error!(error = %e, label, "historian: serialisation failed"),
    }
}

fn today_str() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::writer::JsonlWriter;

    #[test]
    fn jsonl_writer_roundtrip() {
        let tmp = std::env::temp_dir().join(format!("historian_test_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).expect("create tmp dir");

        let mut w = JsonlWriter::new("test", tmp.clone(), "2026-01-01").expect("create writer");
        w.write_line("2026-01-01", r#"{"a":1}"#).expect("write 1");
        w.write_line("2026-01-01", r#"{"a":2}"#).expect("write 2");

        let path = tmp.join("test_2026-01-01.jsonl");
        let content = std::fs::read_to_string(&path).expect("read file");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], r#"{"a":1}"#);
        assert_eq!(lines[1], r#"{"a":2}"#);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn jsonl_writer_day_rollover() {
        let tmp =
            std::env::temp_dir().join(format!("historian_test_rollover_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).expect("create tmp dir");

        let mut w = JsonlWriter::new("test", tmp.clone(), "2026-01-01").expect("create writer");
        w.write_line("2026-01-01", r#"{"day":1}"#)
            .expect("write day 1");
        // Simulate midnight rollover
        w.write_line("2026-01-02", r#"{"day":2}"#)
            .expect("write day 2");

        let day1 = std::fs::read_to_string(tmp.join("test_2026-01-01.jsonl")).expect("read day 1");
        let day2 = std::fs::read_to_string(tmp.join("test_2026-01-02.jsonl")).expect("read day 2");
        assert_eq!(day1.trim(), r#"{"day":1}"#);
        assert_eq!(day2.trim(), r#"{"day":2}"#);

        std::fs::remove_dir_all(&tmp).ok();
    }
}
