//! The daemon's log subscriber: one line per event, as text or JSON.

use tracing::Subscriber;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

/// How log lines are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum LogFormat {
    /// Human lines.
    Text,
    /// One JSON object per line: `timestamp`, `level`, `target`,
    /// `message`, the event's fields, and `span` with the call's
    /// `method` and `peer` inside an RPC.
    Json,
}

/// A subscriber writing `format` through `writer`, filtered by `filter`.
pub fn subscriber<W>(
    format: LogFormat,
    filter: EnvFilter,
    writer: W,
) -> Box<dyn Subscriber + Send + Sync>
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer);
    match format {
        LogFormat::Text => Box::new(builder.finish()),
        LogFormat::Json => Box::new(
            builder
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(false)
                .finish(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Buf {
        type Writer = Buf;

        fn make_writer(&'a self) -> Buf {
            self.clone()
        }
    }

    #[test]
    fn json_lines_carry_the_fields_and_the_span() {
        let buf = Buf::default();
        let sub = subscriber(LogFormat::Json, EnvFilter::new("info"), buf.clone());
        tracing::subscriber::with_default(sub, || {
            let span =
                tracing::info_span!("rpc", method = "/chron.v1.Chron/Commit", peer = "1.2.3.4:5");
            let _g = span.enter();
            tracing::info!(status = 0, duration_ms = 3u64, "done");
        });
        let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let line: serde_json::Value = serde_json::from_str(text.trim()).expect("one JSON line");
        assert_eq!(line["message"], "done");
        assert_eq!(line["level"], "INFO");
        assert_eq!(line["status"], 0);
        assert_eq!(line["duration_ms"], 3);
        assert_eq!(line["span"]["method"], "/chron.v1.Chron/Commit");
        assert_eq!(line["span"]["peer"], "1.2.3.4:5");
        assert!(line["timestamp"].is_string());
        assert!(line["target"].is_string());

        let buf = Buf::default();
        let sub = subscriber(LogFormat::Text, EnvFilter::new("info"), buf.clone());
        tracing::subscriber::with_default(sub, || tracing::info!("plain"));
        let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(text.trim()).is_err(),
            "text is not JSON"
        );
        assert!(text.contains("plain"));
    }
}
