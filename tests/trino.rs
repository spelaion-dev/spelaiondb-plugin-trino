//! Tests against real Trino and Presto servers.
//!
//! They need `SPELAION_TEST_TRINO=host:port` and `SPELAION_TEST_PRESTO=host:port`. Locally:
//! `ci/services.sh` and
//! `SPELAION_TEST_TRINO=127.0.0.1:18080 SPELAION_TEST_PRESTO=127.0.0.1:18081 cargo test`.
//! Without the variables they are skipped with a notice, which does not count as tested. Every
//! test runs on each server it has.

use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType,
    ObjectKind, ResultSchema, TlsMode, TransactionState, Value,
};
use spelaion_driver_trino::TrinoDriver;

/// The servers there are, each with its driver.
fn servers() -> Vec<(&'static str, TrinoDriver, String, u16)> {
    let mut out = Vec::new();
    for (variable, name, driver) in [
        ("SPELAION_TEST_TRINO", "trino", TrinoDriver::trino()),
        ("SPELAION_TEST_PRESTO", "presto", TrinoDriver::presto()),
    ] {
        match std::env::var(variable) {
            Ok(addr) => {
                let (host, port) = addr.rsplit_once(':').expect("host:port");
                out.push((
                    name,
                    driver,
                    host.to_string(),
                    port.parse().expect("a port"),
                ));
            }
            Err(_) => eprintln!("{variable} not set: {name} skipped"),
        }
    }
    out
}

fn params(host: &str, port: u16, database: &str) -> ConnectParams {
    ConnectParams {
        host: host.into(),
        port,
        user: "spelaion".into(),
        database: database.into(),
        tls: TlsMode::Disable,
        ..Default::default()
    }
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

async fn query(conn: &dyn Connection, sql: &str) -> (ResultSchema, Vec<Vec<Value>>) {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
    let mut schema: Option<ResultSchema> = None;
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(s) => schema = Some((*s).clone()),
            ExecEvent::Batch(batch) => {
                let columns = &schema.as_ref().expect("columns come first").columns;
                for r in 0..batch.num_rows() {
                    rows.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                Value::from_array(batch.column(c), r, columns[c].logical)
                                    .expect("a cell")
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (schema.unwrap_or_else(|| panic!("{sql}: no result")), rows)
}

async fn exec(conn: &dyn Connection, sql: &str) {
    run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
}

/// Every type: the cell's text against the server's own (`CAST(… AS VARCHAR)`, or
/// `json_format(CAST(… AS JSON))` for JSON, arrays and maps, `to_hex` for bytes), its logical
/// type and its native type. Doubles are compared as numbers: the server writes them with an
/// exponent (`1.0E-1`), the core as plain numbers.
#[tokio::test]
async fn every_type_reads_as_the_server_writes_it() {
    for (name, driver, host, port) in servers() {
        let conn = driver.connect(&params(&host, port, "")).await.unwrap();
        let conn = conn.as_ref();
        let timestamp = if name == "trino" {
            "timestamp(6)"
        } else {
            "timestamp"
        };
        let zoo: Vec<(&str, LogicalType, &str, &str)> = vec![
            ("true", LogicalType::Bool, "boolean", "CAST(v AS VARCHAR)"),
            (
                "TINYINT '-128'",
                LogicalType::Int,
                "tinyint",
                "CAST(v AS VARCHAR)",
            ),
            (
                "SMALLINT '32767'",
                LogicalType::Int,
                "smallint",
                "CAST(v AS VARCHAR)",
            ),
            (
                "INTEGER '-2147483648'",
                LogicalType::Int,
                "integer",
                "CAST(v AS VARCHAR)",
            ),
            (
                "BIGINT '9223372036854775807'",
                LogicalType::Int,
                "bigint",
                "CAST(v AS VARCHAR)",
            ),
            (
                "DECIMAL '-12.34'",
                LogicalType::Decimal,
                "decimal",
                "CAST(v AS VARCHAR)",
            ),
            (
                "DECIMAL '12345678901234567890.123456789'",
                LogicalType::Decimal,
                "decimal",
                "CAST(v AS VARCHAR)",
            ),
            (
                "VARCHAR 'tab\there, ñ and ''quotes'''",
                LogicalType::Text,
                "varchar",
                "CAST(v AS VARCHAR)",
            ),
            (
                "CAST('ab' AS CHAR(4))",
                LogicalType::Text,
                "char(4)",
                "CAST(v AS VARCHAR)",
            ),
            (
                "DATE '2024-02-29'",
                LogicalType::Date,
                "date",
                "CAST(v AS VARCHAR)",
            ),
            (
                "DATE '1900-01-01'",
                LogicalType::Date,
                "date",
                "CAST(v AS VARCHAR)",
            ),
            (
                "TIME '23:59:58.123'",
                LogicalType::Native,
                "time",
                "CAST(v AS VARCHAR)",
            ),
            (
                "TIMESTAMP '2024-02-29 23:59:58.123456'",
                LogicalType::Native,
                timestamp,
                "CAST(v AS VARCHAR)",
            ),
            (
                "TIMESTAMP '2024-02-29 23:59:58.123 UTC'",
                LogicalType::Native,
                "timestamp",
                "CAST(v AS VARCHAR)",
            ),
            (
                "UUID '61f0c404-5cb3-11e7-907b-a6006ad3dba0'",
                LogicalType::Uuid,
                "uuid",
                "CAST(v AS VARCHAR)",
            ),
            (
                "INTERVAL '3' DAY",
                LogicalType::Native,
                "interval day to second",
                "CAST(v AS VARCHAR)",
            ),
            (
                "INTERVAL '2' MONTH",
                LogicalType::Native,
                "interval year to month",
                "CAST(v AS VARCHAR)",
            ),
            (
                "IPADDRESS '10.0.0.1'",
                LogicalType::Native,
                "ipaddress",
                "CAST(v AS VARCHAR)",
            ),
            (
                "JSON '{\"a\": [1, 2], \"b\": \"x\"}'",
                LogicalType::Json,
                "json",
                "json_format(v)",
            ),
            (
                "ARRAY[1, 2, NULL]",
                LogicalType::Array,
                "array(integer)",
                "json_format(CAST(v AS JSON))",
            ),
            (
                "ARRAY['a', 'b''c']",
                LogicalType::Array,
                "array(varchar",
                "json_format(CAST(v AS JSON))",
            ),
            (
                "MAP(ARRAY['k'], ARRAY[1])",
                LogicalType::Native,
                "map(varchar",
                "json_format(CAST(v AS JSON))",
            ),
            ("X'0001ff'", LogicalType::Bytes, "varbinary", "to_hex(v)"),
        ];
        for (expr, logical, native, server) in zoo {
            let sql = format!("SELECT v, {server} FROM (SELECT {expr} AS v)");
            let (schema, rows) = query(conn, &sql).await;
            let column = &schema.columns[0];
            assert!(
                column.native_type.to_ascii_lowercase().starts_with(native),
                "{name}: {expr}: {}",
                column.native_type
            );
            assert_eq!(column.logical, logical, "{name}: {expr}");
            let Value::Text(server_text) = &rows[0][1] else {
                panic!("{name}: {expr}: {:?}", rows[0][1]);
            };
            let cell = match &rows[0][0] {
                Value::Bytes(bytes) => bytes.iter().map(|b| format!("{b:02X}")).collect(),
                other => other.to_string(),
            };
            if server.starts_with("json_format") {
                // The same JSON: Presto spaces its arrays (`[ 1, 2 ]`), json_format does not.
                let parsed = |text: &str| -> serde_json::Value {
                    serde_json::from_str(text)
                        .unwrap_or_else(|e| panic!("{name}: {expr}: {text}: {e}"))
                };
                assert_eq!(parsed(&cell), parsed(server_text), "{name}: {expr}");
            } else {
                assert_eq!(&cell, server_text, "{name}: {expr}");
            }
        }
        // Doubles and reals, as numbers; NaN and the infinities.
        let (schema, rows) = query(
            conn,
            "SELECT REAL '0.1', DOUBLE '-1234.5625', DOUBLE '1e30', nan(), infinity(), -infinity()",
        )
        .await;
        assert!(
            schema
                .columns
                .iter()
                .all(|c| c.logical == LogicalType::Float)
        );
        let numbers: Vec<f64> = rows[0]
            .iter()
            .map(|v| match v {
                Value::Float(x) => *x,
                other => panic!("{name}: {other:?}"),
            })
            .collect();
        assert_eq!(&numbers[..3], [0.1, -1234.5625, 1e30]);
        assert!(numbers[3].is_nan());
        assert_eq!(&numbers[4..], [f64::INFINITY, f64::NEG_INFINITY]);
        // A row is the protocol's array.
        let (_, rows) = query(conn, "SELECT ROW(1, 'a')").await;
        let row: serde_json::Value = serde_json::from_str(&rows[0][0].to_string()).unwrap();
        assert_eq!(row, serde_json::json!([1, "a"]), "{name}");
        // NULL is NULL, whatever the type.
        let (_, rows) = query(
            conn,
            "SELECT CAST(NULL AS integer), CAST(NULL AS varchar), CAST(NULL AS date), \
             CAST(NULL AS double), CAST(NULL AS boolean), CAST(NULL AS varbinary), CAST(NULL AS array(integer))",
        )
        .await;
        assert!(
            rows[0].iter().all(|v| *v == Value::Null),
            "{name}: {:?}",
            rows[0]
        );
    }
}

/// The session lives in the client: what the server says to set holds for the next statements.
#[tokio::test]
async fn the_session_keeps_what_statements_set() {
    for (name, driver, host, port) in servers() {
        let conn = driver
            .connect(&params(&host, port, "tpch.tiny"))
            .await
            .unwrap();
        let conn = conn.as_ref();
        assert_eq!(
            conn.current_database().await.unwrap().as_deref(),
            Some("tpch")
        );
        let (_, rows) = query(conn, "SELECT count(*) FROM nation").await;
        assert_eq!(rows[0][0], Value::Int(25), "{name}");
        exec(conn, "USE memory.default").await;
        assert_eq!(
            conn.current_database().await.unwrap().as_deref(),
            Some("memory")
        );
        // An unqualified name is now memory.default's.
        let table = format!("session_{name}_{}", std::process::id());
        exec(conn, &format!("CREATE TABLE {table} AS SELECT 7 AS a")).await;
        let (_, rows) = query(conn, &format!("SELECT a FROM memory.default.{table}")).await;
        assert_eq!(rows[0][0], Value::Int(7), "{name}");
        exec(conn, &format!("DROP TABLE {table}")).await;
        exec(conn, "SET SESSION query_max_run_time = '5m'").await;
        let (_, rows) = query(conn, "SHOW SESSION LIKE 'query_max_run_time'").await;
        assert_eq!(rows[0][1].to_string(), "5m", "{name}");
        exec(conn, "PREPARE twice FROM SELECT ? * 2").await;
        let (_, rows) = query(conn, "EXECUTE twice USING 21").await;
        assert_eq!(rows[0][0], Value::Int(42), "{name}");
        conn.select_database("system").await.unwrap();
        assert_eq!(
            conn.current_database().await.unwrap().as_deref(),
            Some("system")
        );
        assert!(!conn.server_version().await.unwrap().is_empty());
    }
}

/// SpelaionDB's read-only barrier on these engines (ADR-020 there): a read-only transaction,
/// which the server enforces. The transaction id is the client's, so its state is known.
#[tokio::test]
async fn a_read_only_transaction_refuses_writes_in_the_server() {
    for (name, driver, host, port) in servers() {
        let conn = driver
            .connect(&params(&host, port, "memory.default"))
            .await
            .unwrap();
        let conn = conn.as_ref();
        let table = format!("ro_{name}_{}", std::process::id());
        let _ = run(conn, &format!("DROP TABLE IF EXISTS {table}")).await;
        exec(conn, &format!("CREATE TABLE {table} (a integer)")).await;
        assert_eq!(
            conn.transaction_state().await.unwrap(),
            TransactionState::Idle
        );
        // Each write in a transaction of its own: a refused one aborts its transaction.
        for sql in [
            format!("INSERT INTO {table} VALUES (1)"),
            format!("CREATE TABLE {table}_copy AS SELECT * FROM {table}"),
            format!("DROP TABLE {table}"),
        ] {
            exec(conn, "START TRANSACTION READ ONLY").await;
            assert_eq!(
                conn.transaction_state().await.unwrap(),
                TransactionState::Open
            );
            let (_, rows) = query(conn, &format!("SELECT count(*) FROM {table}")).await;
            assert_eq!(rows[0][0], Value::Int(0));
            match run(conn, &sql).await {
                Err(DriverError::Database { code, .. }) => {
                    assert_eq!(
                        code.as_deref(),
                        Some("READ_ONLY_VIOLATION"),
                        "{name}: {sql}"
                    );
                }
                other => panic!("{name}: {sql}: {other:?}"),
            }
            exec(conn, "ROLLBACK").await;
            assert_eq!(
                conn.transaction_state().await.unwrap(),
                TransactionState::Idle
            );
        }
        let events = run(conn, &format!("INSERT INTO {table} VALUES (1), (2)"))
            .await
            .unwrap();
        assert!(
            matches!(
                events.as_slice(),
                [ExecEvent::Done {
                    rows_affected: Some(2)
                }]
            ),
            "{name}: {events:?}"
        );
        exec(conn, &format!("DROP TABLE {table}")).await;
    }
}

#[tokio::test]
async fn a_large_result_comes_in_batches() {
    for (name, driver, host, port) in servers() {
        let conn = driver
            .connect(&params(&host, port, "tpch.sf1"))
            .await
            .unwrap();
        let opts = ExecOptions {
            batch_size: 10_000,
            ..Default::default()
        };
        let events: Vec<ExecEvent> = conn
            .execute("SELECT orderkey, comment FROM orders LIMIT 200000", &opts)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let rows: usize = events
            .iter()
            .map(|e| match e {
                ExecEvent::Batch(b) => {
                    assert!(b.num_rows() <= 10_000);
                    b.num_rows()
                }
                _ => 0,
            })
            .sum();
        assert_eq!(rows, 200_000, "{name}");
        assert!(matches!(events.first(), Some(ExecEvent::Columns(_))));
        assert!(matches!(events.last(), Some(ExecEvent::Done { .. })));
    }
}

const SLOW: &str = "SELECT count(*) FROM UNNEST(sequence(1, 10000)) a(x) \
                    CROSS JOIN UNNEST(sequence(1, 10000)) b(y) CROSS JOIN UNNEST(sequence(1, 10000)) c(z)";

#[tokio::test]
async fn a_cancel_stops_the_query_on_the_server() {
    for (name, driver, host, port) in servers() {
        let conn = driver.connect(&params(&host, port, "")).await.unwrap();
        let cancel = conn.cancel_handle();
        let started = Instant::now();
        let (result, ()) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(30), run(conn.as_ref(), SLOW)),
            async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                cancel.cancel().await.unwrap();
            }
        );
        let result = result.expect("the cancel was obeyed");
        assert!(
            matches!(result, Err(DriverError::Cancelled)),
            "{name}: {result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(25));
        // Nothing sent after the cancel is touched, and the query is not running any more.
        let (_, rows) = query(
            conn.as_ref(),
            &format!(
                "SELECT count(*) FROM system.runtime.queries WHERE state = 'RUNNING' AND query = '{}'",
                SLOW.replace('\'', "''")
            ),
        )
        .await;
        assert_eq!(rows[0][0], Value::Int(0), "{name}");
    }
}

#[tokio::test]
async fn refusals_carry_the_servers_error_name() {
    for (name, driver, host, port) in servers() {
        let conn = driver
            .connect(&params(&host, port, "memory.default"))
            .await
            .unwrap();
        // Presto names a missing table a syntax error.
        let missing = if name == "trino" {
            "TABLE_NOT_FOUND"
        } else {
            "SYNTAX_ERROR"
        };
        match run(conn.as_ref(), "SELECT * FROM a_table_that_is_not_there").await {
            Err(DriverError::Database { code, .. }) => {
                assert_eq!(code.as_deref(), Some(missing), "{name}");
            }
            other => panic!("{name}: {other:?}"),
        }
        match run(conn.as_ref(), "SELECT 1 / 0").await {
            Err(DriverError::Database { code, .. }) => {
                assert_eq!(code.as_deref(), Some("DIVISION_BY_ZERO"), "{name}");
            }
            other => panic!("{name}: {other:?}"),
        }
    }
}

/// Catalogs, then schemas, then tables: the explorer's three levels.
#[tokio::test]
async fn the_explorer_reads_catalogs_schemas_and_tables() {
    for (name, driver, host, port) in servers() {
        let conn = driver
            .connect(&params(&host, port, "memory"))
            .await
            .unwrap();
        let conn = conn.as_ref();
        let schema = format!("explored_{name}_{}", std::process::id());
        let _ = run(
            conn,
            &format!("DROP SCHEMA IF EXISTS memory.{schema} CASCADE"),
        )
        .await;
        exec(conn, &format!("CREATE SCHEMA memory.{schema}")).await;
        exec(
            conn,
            &format!("CREATE TABLE memory.{schema}.events (id bigint, note varchar)"),
        )
        .await;
        exec(
            conn,
            &format!("CREATE VIEW memory.{schema}.recent AS SELECT * FROM memory.{schema}.events"),
        )
        .await;

        let catalogs = conn.list_databases().await.unwrap();
        assert!(
            catalogs.contains(&"memory".to_string()) && catalogs.contains(&"tpch".to_string()),
            "{name}: {catalogs:?}"
        );
        let schemas = conn.list_schemas().await.unwrap();
        assert!(
            schemas.iter().any(|s| s.name == schema && !s.system),
            "{name}"
        );
        assert!(
            schemas
                .iter()
                .any(|s| s.name == "information_schema" && s.system)
        );
        let objects = conn.list_objects(&schema).await.unwrap();
        let found: Vec<(&str, ObjectKind)> =
            objects.iter().map(|o| (o.name.as_str(), o.kind)).collect();
        assert_eq!(
            found,
            [("events", ObjectKind::Table), ("recent", ObjectKind::View)],
            "{name}"
        );
        let table = conn.describe_table(&schema, "events").await.unwrap();
        let columns: Vec<(&str, &str, bool)> = table
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.native_type.as_str(), c.nullable))
            .collect();
        assert_eq!(
            columns,
            [("id", "bigint", true), ("note", "varchar", true)],
            "{name}"
        );
        assert!(table.primary_key.is_empty());
        assert_eq!(conn.list_columns(&schema).await.unwrap().len(), 4);
        let definition = conn.relation_definition(&schema, "recent").await.unwrap();
        assert!(
            definition.to_ascii_uppercase().starts_with("CREATE VIEW"),
            "{name}: {definition}"
        );
        assert!(definition.ends_with(';'));
        // A name with quotes is written as a literal, not as SQL.
        assert!(
            conn.list_objects("no'such\"schema")
                .await
                .unwrap()
                .is_empty()
        );
        exec(conn, &format!("DROP VIEW memory.{schema}.recent")).await;
        exec(conn, &format!("DROP TABLE memory.{schema}.events")).await;
        exec(conn, &format!("DROP SCHEMA memory.{schema}")).await;
    }
}

#[tokio::test]
async fn connecting_to_the_address_given_keeps_the_host_name() {
    for (name, driver, _, port) in servers() {
        let mut p = params("trino.invalid", 1, "");
        p.connect_addr = Some(format!("127.0.0.1:{port}").parse().unwrap());
        let conn = driver
            .connect(&p)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let (_, rows) = query(conn.as_ref(), "SELECT 1").await;
        assert_eq!(rows.len(), 1);
    }
}

#[tokio::test]
async fn a_catalog_that_is_not_there_is_a_connection_error() {
    for (name, driver, host, port) in servers() {
        match driver
            .connect(&params(&host, port, "nowhere_to_be_found"))
            .await
        {
            Err(DriverError::Connection(_)) => {}
            Err(other) => panic!("{name}: {other:?}"),
            Ok(_) => panic!("{name}: connected to a catalog that is not there"),
        }
    }
}

#[tokio::test]
async fn a_password_is_never_sent_without_tls() {
    for (name, driver, host, port) in servers() {
        let mut p = params(&host, port, "");
        p.password = Some(spelaion_driver_api::Secret::new("never-sent-4b1d"));
        match driver.connect(&p).await {
            Err(DriverError::Connection(message)) => {
                assert!(message.contains("TLS"), "{name}: {message}");
                assert!(!message.contains("never-sent-4b1d"));
            }
            Err(other) => panic!("{name}: {other:?}"),
            Ok(_) => panic!("{name}: a password went over plain HTTP"),
        }
    }
}
