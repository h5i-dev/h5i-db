//! Integration tests for the native SQL kernel.
//!
//! These need no Jupyter kernel: the whole point of the backend is that a
//! query never leaves the process. They build a real h5i-db database and run
//! real DataFusion plans against it, so they run by default.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{Database, TableOptions, WriteOptions};
use h5i_db_notebook::document::Output;
use h5i_db_notebook::kernel::sql::{SqlKernel, SqlOptions};
use h5i_db_notebook::kernel::{Completeness, ExecOptions, ExecStatus, Kernel, NullSink};
use tempfile::TempDir;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
    ]))
}

/// A database with `rows` trades split across three symbols.
async fn fixture(rows: usize) -> (TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("market.db");
    let db = Arc::new(Database::create(&path).await.unwrap());
    db.create_table(
        "trades",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let symbols = ["AAPL", "MSFT", "NVDA"];
    let base = 1_700_000_000_000_000_000i64;
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(arrow::array::TimestampNanosecondArray::from(
                (0..rows)
                    .map(|i| base + i as i64 * 1_000_000)
                    .collect::<Vec<i64>>(),
            )),
            Arc::new(StringArray::from(
                (0..rows).map(|i| symbols[i % 3]).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..rows)
                    .map(|i| 100.0 + (i % 50) as f64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..rows).map(|i| (i % 7) as i64 + 1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    db.write("trades", vec![batch], WriteOptions::default())
        .await
        .unwrap();
    (dir, path)
}

async fn kernel(rows: usize) -> (TempDir, SqlKernel) {
    let (dir, path) = fixture(rows).await;
    let kernel = SqlKernel::open(&path, None, SqlOptions::default())
        .await
        .expect("failed to open the SQL kernel");
    (dir, kernel)
}

fn options() -> ExecOptions {
    ExecOptions {
        timeout: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    }
}

fn result_text(outputs: &[Output]) -> String {
    outputs
        .iter()
        .find_map(|o| match o {
            Output::ExecuteResult(r) => r.data.text_plain().map(str::to_string),
            _ => None,
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn a_query_returns_an_aligned_table_and_its_shape() {
    let (_dir, mut kernel) = kernel(99).await;
    let outcome = kernel
        .execute(
            "SELECT symbol, count(*) AS n FROM trades GROUP BY symbol ORDER BY symbol",
            &options(),
            &mut NullSink,
        )
        .await
        .unwrap();
    assert_eq!(outcome.status, ExecStatus::Ok);
    let text = result_text(&outcome.outputs);
    assert!(text.contains("AAPL"), "{text}");
    assert!(text.contains("[3 rows x 2 columns]"), "{text}");
}

#[tokio::test]
async fn a_large_result_is_previewed_rather_than_dumped() {
    // Rendering ten thousand rows into a terminal is the failure mode this
    // backend exists to avoid.
    let (_dir, mut kernel) = kernel(5000).await;
    let outcome = kernel
        .execute("SELECT * FROM trades", &options(), &mut NullSink)
        .await
        .unwrap();
    let text = result_text(&outcome.outputs);
    assert!(
        text.contains("[showing 20 of 5000 rows x 4 columns]"),
        "{text}"
    );
    assert!(
        text.lines().count() < 30,
        "preview was {} lines long",
        text.lines().count()
    );
}

#[tokio::test]
async fn the_row_cap_is_reported_rather_than_silently_applied() {
    let (_dir, path) = fixture(500).await;
    let mut kernel = SqlKernel::open(
        &path,
        None,
        SqlOptions {
            max_rows: 10,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let outcome = kernel
        .execute("SELECT * FROM trades", &options(), &mut NullSink)
        .await
        .unwrap();
    let warning = outcome
        .outputs
        .iter()
        .find_map(|o| match o {
            Output::Stream(s) => Some(s.text.clone()),
            _ => None,
        })
        .expect("truncation must be announced, not silent");
    assert!(warning.contains("first 10 rows"), "{warning}");
    assert!(result_text(&outcome.outputs).contains("10+ rows"));
}

#[tokio::test]
async fn a_bad_query_becomes_a_cell_error_not_a_tool_failure() {
    let (_dir, mut kernel) = kernel(10).await;
    let outcome = kernel
        .execute("SELECT * FROM nope", &options(), &mut NullSink)
        .await
        .expect("a bad query must not fail the call");
    assert_eq!(outcome.status, ExecStatus::Error);
    let error = outcome.error().expect("no error output");
    assert_eq!(error.ename, "SqlError");
    assert!(error.evalue.contains("nope"), "{}", error.evalue);

    // And the kernel is still usable, exactly like a Python traceback.
    let after = kernel
        .execute("SELECT 1 AS ok", &options(), &mut NullSink)
        .await
        .unwrap();
    assert_eq!(after.status, ExecStatus::Ok);
}

#[tokio::test]
async fn results_carry_html_so_the_notebook_renders_in_jupyter() {
    let (_dir, mut kernel) = kernel(10).await;
    let outcome = kernel
        .execute(
            "SELECT symbol FROM trades LIMIT 2",
            &options(),
            &mut NullSink,
        )
        .await
        .unwrap();
    let bundle = outcome
        .outputs
        .iter()
        .find_map(|o| match o {
            Output::ExecuteResult(r) => Some(&r.data),
            _ => None,
        })
        .unwrap();
    let html = bundle.text_of("text/html").expect("no html representation");
    assert!(html.contains("<table"), "{html}");
    assert!(html.contains("<th>symbol</th>"), "{html}");
}

#[tokio::test]
async fn execution_counts_advance() {
    let (_dir, mut kernel) = kernel(10).await;
    let first = kernel
        .execute("SELECT 1", &options(), &mut NullSink)
        .await
        .unwrap();
    let second = kernel
        .execute("SELECT 2", &options(), &mut NullSink)
        .await
        .unwrap();
    assert_eq!(first.execution_count, Some(1));
    assert_eq!(second.execution_count, Some(2));
}

#[tokio::test]
async fn the_handoff_file_holds_every_row_not_just_the_preview() {
    // The cap is a display decision; binding a truncated frame into Python
    // would be a correctness bug.
    let (_dir, path) = fixture(500).await;
    let kernel = SqlKernel::open(
        &path,
        None,
        SqlOptions {
            max_rows: 10,
            display_rows: 5,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let out = tempfile::NamedTempFile::new().unwrap();
    let rows = kernel
        .write_ipc("SELECT * FROM trades", out.path())
        .await
        .unwrap();
    assert_eq!(rows, 500, "the handoff was truncated to the display cap");

    // And it is readable Arrow IPC, not just bytes we wrote.
    let file = std::fs::File::open(out.path()).unwrap();
    let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
    let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(total, 500);
}

#[tokio::test]
async fn completion_offers_table_names_and_keywords() {
    let (_dir, mut kernel) = kernel(10).await;
    let code = "SELECT * FROM tra";
    let completions = kernel.complete(code, code.len()).await.unwrap();
    assert!(
        completions.matches.iter().any(|m| m == "trades"),
        "{:?}",
        completions.matches
    );
    assert_eq!(completions.cursor_end, code.len());
    assert_eq!(
        &code[completions.cursor_start..completions.cursor_end],
        "tra"
    );

    let code = "SEL";
    let keywords = kernel.complete(code, code.len()).await.unwrap();
    assert!(
        keywords.matches.iter().any(|m| m == "SELECT"),
        "{:?}",
        keywords.matches
    );
}

#[tokio::test]
async fn inspecting_a_table_shows_its_schema() {
    let (_dir, mut kernel) = kernel(10).await;
    let bundle = kernel
        .inspect("trades", 3, 0)
        .await
        .unwrap()
        .expect("trades should be introspectable");
    let text = bundle.text_plain().unwrap();
    assert!(text.contains("symbol"), "{text}");
    assert!(text.contains("price"), "{text}");

    assert!(
        kernel.inspect("not_a_table", 3, 0).await.unwrap().is_none(),
        "an unknown name must report nothing rather than guess"
    );
}

#[tokio::test]
async fn is_complete_tracks_open_parentheses_and_strings() {
    let (_dir, mut kernel) = kernel(10).await;
    assert_eq!(
        kernel.is_complete("SELECT 1;").await.unwrap(),
        Completeness::Complete
    );
    assert_eq!(
        kernel.is_complete("SELECT 1").await.unwrap(),
        Completeness::Complete
    );
    assert!(matches!(
        kernel.is_complete("SELECT count(").await.unwrap(),
        Completeness::Incomplete { .. }
    ));
    assert!(matches!(
        kernel.is_complete("SELECT 'abc").await.unwrap(),
        Completeness::Incomplete { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupt_stops_a_running_query() {
    // A cross join over ten thousand rows is a hundred million pairs: it
    // cannot finish inside the interrupt window, so the test is about the
    // interrupt landing rather than about a race.
    let (_dir, mut kernel) = kernel(10_000).await;
    let handle = kernel.interrupt_handle();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        handle.interrupt();
    });

    let started = std::time::Instant::now();
    let error = match kernel
        .execute(
            "SELECT count(*) FROM trades a, trades b WHERE a.price < b.price",
            &options(),
            &mut NullSink,
        )
        .await
    {
        Err(error) => error,
        Ok(outcome) => panic!("the query was not interrupted: {outcome:?}"),
    };
    assert_eq!(error.code(), "interrupted");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "interrupt took {:?}",
        started.elapsed()
    );

    // The flag is cleared, so the next cell runs normally.
    let after = kernel
        .execute("SELECT 1", &options(), &mut NullSink)
        .await
        .unwrap();
    assert_eq!(after.status, ExecStatus::Ok);
}

#[tokio::test]
async fn an_interrupt_arriving_between_cells_does_not_kill_the_next_one() {
    // Ctrl-C while nothing is running must not poison the following cell.
    let (_dir, mut kernel) = kernel(10).await;
    kernel.interrupt_handle().interrupt();
    let outcome = kernel
        .execute("SELECT 1 AS ok", &options(), &mut NullSink)
        .await
        .expect("a stale interrupt should not fail the next cell");
    assert_eq!(outcome.status, ExecStatus::Ok);
}

#[tokio::test]
async fn restart_rebuilds_the_session_and_resets_numbering() {
    let (_dir, mut kernel) = kernel(10).await;
    kernel
        .execute("SELECT 1", &options(), &mut NullSink)
        .await
        .unwrap();
    kernel.restart().await.unwrap();
    let after = kernel
        .execute("SELECT 1", &options(), &mut NullSink)
        .await
        .unwrap();
    assert_eq!(after.execution_count, Some(1));
    assert_eq!(after.status, ExecStatus::Ok);
}

#[tokio::test]
async fn opening_a_missing_database_says_so() {
    let error =
        match SqlKernel::open("/definitely/not/a/database", None, SqlOptions::default()).await {
            Err(error) => error,
            Ok(_) => panic!("opening a nonexistent database should fail"),
        };
    assert_eq!(error.code(), "invalid_input");
    assert!(
        error.to_string().contains("cannot open database"),
        "{error}"
    );
}
