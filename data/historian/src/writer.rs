use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Context;

/// Appends newline-delimited JSON to a daily file, one line per record.
///
/// Each `write_line` call issues a single `write` syscall via O_APPEND —
/// no userspace buffering, no background flush task. A crash can corrupt at
/// most the in-flight line; all prior lines are intact and readable:
///
///   pd.read_json('brti_2026-04-07.jsonl', lines=True)
///
/// Day rollover is handled automatically: when `day` changes, the writer
/// reopens a new file transparently.
pub struct JsonlWriter {
    file: File,
    day: String,
    prefix: &'static str,
    log_dir: PathBuf,
}

impl JsonlWriter {
    pub fn new(prefix: &'static str, log_dir: PathBuf, day: &str) -> anyhow::Result<Self> {
        let file = open_append(&log_dir, prefix, day)?;
        Ok(JsonlWriter {
            file,
            day: day.to_string(),
            prefix,
            log_dir,
        })
    }

    /// Append `line` + newline to today's file, rolling to a new file at midnight.
    pub fn write_line(&mut self, day: &str, line: &str) -> anyhow::Result<()> {
        if day != self.day {
            self.file = open_append(&self.log_dir, self.prefix, day)?;
            self.day = day.to_string();
        }
        // Build the complete line in one allocation so write_all is a single syscall.
        // O_APPEND guarantees atomicity for writes ≤ PIPE_BUF (~4 KB); our lines are < 1 KB.
        let mut buf = Vec::with_capacity(line.len() + 1);
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
        self.file.write_all(&buf).context("write jsonl line")
    }
}

fn open_append(log_dir: &Path, prefix: &'static str, day: &str) -> anyhow::Result<File> {
    let path = log_dir.join(format!("{}_{}.jsonl", prefix, day));
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open jsonl file {path:?}"))
}
