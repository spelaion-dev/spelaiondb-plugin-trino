//! The driver of a made-up engine, `example`: what a plugin's repository starts from.
//!
//! **Replace this file with your engine's driver.** A driver implements `Driver` and
//! `Connection` of `spelaion-driver-api` (SpelaionDB's ADR-005): it connects, runs one statement
//! at a time and returns its results as Arrow batches, cancels, and tells what the database
//! holds. It never decides what may run: SpelaionDB's core splits, classifies and guards
//! statements before one reaches a driver.
//!
//! This engine has no server. It knows three statements, enough for the protocol's conformance
//! set (`tests/conformance.rs`): `select 1`, `wait` (which runs until it is cancelled) and
//! anything else, which it refuses as a server would.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use async_trait::async_trait;
use futures_util::stream;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ColumnMeta, ConnectParams, Connection, Driver, DriverError,
    EventStream, ExecEvent, ExecOptions, LogicalType, ObjectInfo, Result, ResultSchema, SchemaInfo,
    TableInfo,
};
use tokio::sync::Notify;

/// The `example` engine.
#[derive(Debug, Default, Clone)]
pub struct ExampleDriver;

#[async_trait]
impl Driver for ExampleDriver {
    /// The name SpelaionDB's configuration uses: `engine = "example"`.
    fn engine(&self) -> &str {
        "example"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        // A real driver connects to `params.connect_addr` when it is there (the local end of a
        // tunnel SpelaionDB opened) and to `params.host` and `params.port` otherwise, and
        // verifies TLS against `params.host`. The password is in `params.password`: it is
        // kept in memory for as long as it is needed and written nowhere.
        if params.database == "nowhere" {
            return Err(DriverError::Connection(
                "there is no database called nowhere".into(),
            ));
        }
        Ok(Box::new(ExampleConnection {
            capabilities: Capabilities {
                cancel: true,
                ..Capabilities::default()
            },
            cancelled: Arc::new(Notify::new()),
        }))
    }
}

struct ExampleConnection {
    capabilities: Capabilities,
    /// Told when the statement that is running must stop.
    cancelled: Arc<Notify>,
}

#[async_trait]
impl Connection for ExampleConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// One statement: its columns, its rows in batches, then how it ended.
    async fn execute(&self, sql: &str, _opts: &ExecOptions) -> Result<EventStream> {
        match sql.trim().trim_end_matches(';').to_lowercase().as_str() {
            "select 1" => {
                let schema = Arc::new(ResultSchema::new(vec![ColumnMeta {
                    name: "?column?".into(),
                    logical: LogicalType::Int,
                    native_type: "integer".into(),
                    native_id: None,
                    origin: None,
                }]));
                let one: ArrayRef = Arc::new(Int64Array::from(vec![1]));
                let batch = RecordBatch::try_new(schema.arrow.clone(), vec![one])
                    .map_err(|e| DriverError::Other(e.to_string()))?;
                Ok(Box::pin(stream::iter([
                    Ok(ExecEvent::Columns(schema)),
                    Ok(ExecEvent::Batch(batch)),
                    Ok(ExecEvent::Done {
                        rows_affected: None,
                    }),
                ])))
            }
            // Runs until it is cancelled, as a long query does.
            "wait" => {
                let cancelled = self.cancelled.clone();
                Ok(Box::pin(stream::once(async move {
                    cancelled.notified().await;
                    Err(DriverError::Cancelled)
                })))
            }
            _ => Err(DriverError::Database {
                code: Some("42601".into()),
                message: format!("the example engine does not understand: {sql}"),
                detail: None,
                hint: Some("it knows `select 1` and `wait`".into()),
                position: None,
            }),
        }
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(Cancel(self.cancelled.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        Ok("example 1.0".into())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: "main".into(),
            system: false,
        }])
    }

    async fn list_objects(&self, _schema: &str) -> Result<Vec<ObjectInfo>> {
        Ok(Vec::new())
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        Err(DriverError::Other(format!(
            "there is no table {schema}.{name}"
        )))
    }
}

struct Cancel(Arc<Notify>);

#[async_trait]
impl CancelHandle for Cancel {
    async fn cancel(&self) -> Result<()> {
        self.0.notify_one();
        Ok(())
    }
}
