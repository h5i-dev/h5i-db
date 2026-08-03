//! Wire protocol between the CLI and a session supervisor.
//!
//! Newline-delimited JSON in both directions. One request per connection,
//! answered by zero or more `Event` frames and exactly one terminal frame
//! (`Result`, `Cells`, `Status`, `Completions`, or `Error`). Streaming
//! events rather than buffering is what lets `exec --stream` show a
//! long-running cell's output while it is still running.

use serde::{Deserialize, Serialize};

use crate::document::Output;
use crate::kernel::{ExecStatus, KernelStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Liveness and kernel state.
    Status,
    /// Append a code cell and run it.
    Exec {
        code: String,
        #[serde(default)]
        timeout_secs: Option<u64>,
        /// Return as soon as the cell is queued. Outputs still land in the
        /// notebook, because the supervisor owns the file.
        #[serde(default)]
        detach: bool,
    },
    /// Run cells already in the notebook.
    Run {
        indices: Vec<usize>,
        #[serde(default)]
        timeout_secs: Option<u64>,
        #[serde(default = "default_true")]
        stop_on_error: bool,
    },
    Interrupt,
    Restart {
        #[serde(default)]
        clear_outputs: bool,
    },
    /// Stop the kernel and exit the supervisor.
    Shutdown,
    Complete {
        code: String,
        cursor: usize,
    },
    Inspect {
        code: String,
        cursor: usize,
        #[serde(default)]
        detail: u8,
    },
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// Streamed while a cell runs.
    Event(StreamEvent),
    /// Terminal frame for `Exec` and `Run`.
    Result {
        cells: Vec<CellReport>,
    },
    Status(SessionInfo),
    Completions {
        matches: Vec<String>,
        cursor_start: usize,
        cursor_end: usize,
    },
    Inspect {
        found: bool,
        text: Option<String>,
    },
    Ok,
    /// Terminal frame for any failure, carrying the CLI's error contract so a
    /// supervisor-side error reaches the caller with its code and hint intact.
    Error {
        code: String,
        message: String,
        retryable: bool,
        hint: Option<String>,
        exit_code: i32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum StreamEvent {
    Stdout {
        text: String,
    },
    Stderr {
        text: String,
    },
    ExecutionCount {
        count: i64,
    },
    KernelStatus {
        status: KernelStatus,
    },
    /// A non-stream output arrived (result, display data, error).
    Output {
        mime_types: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellReport {
    pub index: usize,
    pub cell_id: Option<String>,
    pub status: ExecStatus,
    pub execution_count: Option<i64>,
    pub elapsed_ms: u64,
    pub outputs: Vec<Output>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub notebook: String,
    pub kernel_name: String,
    pub kernel_status: KernelStatus,
    pub cells: usize,
    pub pid: u32,
    pub busy: bool,
    pub idle_seconds: u64,
}

impl Response {
    pub fn from_error(error: &crate::Error) -> Self {
        Response::Error {
            code: error.code().to_string(),
            message: error.to_string(),
            retryable: error.retryable(),
            hint: error.hint(),
            exit_code: error.exit_category().code(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_trip_through_json() {
        let cases = vec![
            Request::Status,
            Request::Exec {
                code: "1+1".into(),
                timeout_secs: Some(30),
                detach: false,
            },
            Request::Run {
                indices: vec![0, 2],
                timeout_secs: None,
                stop_on_error: true,
            },
            Request::Restart {
                clear_outputs: true,
            },
        ];
        for request in cases {
            let text = serde_json::to_string(&request).unwrap();
            assert!(!text.contains('\n'), "framing requires one line per frame");
            let back: Request = serde_json::from_str(&text).unwrap();
            assert_eq!(
                serde_json::to_string(&back).unwrap(),
                text,
                "request did not survive a round trip"
            );
        }
    }

    #[test]
    fn run_defaults_to_stopping_at_the_first_error() {
        let request: Request = serde_json::from_str(r#"{"op":"run","indices":[0]}"#).unwrap();
        match request {
            Request::Run { stop_on_error, .. } => assert!(stop_on_error),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn errors_carry_the_cli_contract() {
        let error = crate::Error::KernelDied {
            detail: String::new(),
        };
        let Response::Error {
            code,
            retryable,
            hint,
            exit_code,
            ..
        } = Response::from_error(&error)
        else {
            panic!("wrong variant");
        };
        assert_eq!(code, "kernel_died");
        assert!(retryable);
        assert!(hint.is_some());
        assert_eq!(exit_code, 5);
    }

    #[test]
    fn responses_serialize_to_single_lines() {
        let response = Response::Event(StreamEvent::Stdout {
            text: "multi\nline\noutput".into(),
        });
        let text = serde_json::to_string(&response).unwrap();
        assert!(
            !text.contains('\n'),
            "embedded newlines would break newline framing: {text}"
        );
    }
}
