//! Trino and Presto for SpelaionDB, served as one plugin (SpelaionDB's ADR-041 and ADR-042).
//!
//! Both speak the same HTTP protocol (Presto is where Trino came from): a statement is posted,
//! then its results are polled through each answer's `nextUri`, a page at a time; a `DELETE` on
//! that URI cancels it on the server. Only the headers' prefix differs (`X-Trino-…`,
//! `X-Presto-…`).
//!
//! The session lives in the client: the catalog, schema, session properties, prepared
//! statements and the open transaction are what the server's answers say to set, and are sent
//! back with every request. So `USE`, `SET SESSION` and `START TRANSACTION` hold for the next
//! statements, as in any engine. The connection's requests go one at a time, through an actor.
//!
//! What they do not have is declared, and SpelaionDB hides it: editing rows (no primary keys),
//! importing rows, binding values. Whether a catalog can be written depends on its connector;
//! SpelaionDB's read-only barrier is a `START TRANSACTION READ ONLY`, which the engine itself
//! enforces (ADR-020 there).

pub mod http;
mod introspect;
pub mod tls;
pub mod types;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use hyper::{Method, StatusCode};
use serde_json::Value as Json;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecEvent, ExecOptions, KeepAlive, ObjectInfo, ReferencingKey, Result, ResultSchema,
    SchemaColumn, SchemaInfo, TableInfo, TransactionState,
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::http::{Client, Endpoint, path_of};
use crate::tls::ClientIdentity;
use crate::types::Batches;

/// How long to wait for the server to stop a query it was asked to cancel.
const CANCEL_WAIT: Duration = Duration::from_secs(10);

/// Which of the two servers: they differ only in the prefix of the protocol's headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Trino,
    Presto,
}

impl Flavor {
    fn engine(self) -> &'static str {
        match self {
            Flavor::Trino => "trino",
            Flavor::Presto => "presto",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Flavor::Trino => "X-Trino-",
            Flavor::Presto => "X-Presto-",
        }
    }
}

/// The engine `flavor`.
#[derive(Debug, Clone)]
pub struct TrinoDriver {
    flavor: Flavor,
}

impl TrinoDriver {
    pub fn trino() -> Self {
        Self {
            flavor: Flavor::Trino,
        }
    }

    pub fn presto() -> Self {
        Self {
            flavor: Flavor::Presto,
        }
    }
}

#[async_trait]
impl Driver for TrinoDriver {
    fn engine(&self) -> &str {
        self.flavor.engine()
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        let identity = match (&params.client_certificate, &params.client_key) {
            (Some(cert), Some(key)) => Some(ClientIdentity { cert, key }),
            (None, None) => None,
            _ => {
                return Err(DriverError::Connection(
                    "a client certificate needs its key, and a key its certificate".into(),
                ));
            }
        };
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), identity)?;
        let user = if params.user.is_empty() {
            "spelaiondb".to_string()
        } else {
            params.user.clone()
        };
        let endpoint = Arc::new(Endpoint::new(
            params.host.clone(),
            params.port,
            params.connect_addr,
            tls,
            params.connect_timeout(),
            &user,
            params.password.clone(),
        ));
        // `catalog` or `catalog.schema`.
        let (catalog, schema) = match params.database.split_once('.') {
            Some((catalog, schema)) => (Some(catalog.to_string()), Some(schema.to_string())),
            None if params.database.is_empty() => (None, None),
            None => (Some(params.database.clone()), None),
        };
        let mut session = Session {
            client: Client::new(endpoint),
            flavor: self.flavor,
            user,
            source: params
                .application_name
                .clone()
                .unwrap_or_else(|| "SpelaionDB".into()),
            catalog,
            schema,
            properties: BTreeMap::new(),
            prepared: BTreeMap::new(),
            path: None,
            transaction: None,
        };
        let check = match (&session.catalog, &session.schema) {
            (Some(_), Some(_)) => "SHOW TABLES",
            (Some(_), None) => "SHOW SCHEMAS",
            _ => "SELECT 1",
        };
        match tokio::time::timeout(params.connect_timeout(), session.rows(check)).await {
            Ok(Ok(_)) => {}
            Ok(Err(DriverError::Database { message, .. })) => {
                return Err(DriverError::Connection(message));
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(DriverError::Connection(format!(
                    "the server did not answer within {} s",
                    params.connect_timeout().as_secs()
                )));
            }
        }
        let closed = Arc::new(AtomicBool::new(false));
        let (cancel_tx, cancel_rx) = watch::channel(0u64);
        let cancel = Arc::new(Cancel {
            sent: AtomicU64::new(0),
            upto: cancel_tx,
        });
        let (requests, receiver) = mpsc::channel(16);
        tokio::spawn(actor(session, receiver, cancel_rx, closed.clone()));
        Ok(Box::new(TrinoConnection {
            capabilities: Capabilities {
                // Multi-statement writes depend on each catalog's connector: the app's
                // transaction controls are not offered. SpelaionDB's read barrier still opens
                // a read-only transaction, which every connector honours.
                transactions: false,
                cancel: true,
                explain: false,
                editable_results: false,
                schemas: true,
                insert_rows: false,
                bound_params: false,
            },
            requests,
            cancel,
            closed,
        }))
    }
}

/// `name` as a quoted identifier.
pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `text` as a string literal.
pub fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The client's session: what the server's answers said to set, sent with every request.
struct Session {
    client: Client,
    flavor: Flavor,
    user: String,
    source: String,
    catalog: Option<String>,
    schema: Option<String>,
    /// Session properties, as the server sent them (already encoded).
    properties: BTreeMap<String, String>,
    /// Prepared statements: name → encoded SQL.
    prepared: BTreeMap<String, String>,
    path: Option<String>,
    transaction: Option<String>,
}

/// One page of a statement's results.
struct Page {
    columns: Option<Vec<(String, String)>>,
    data: Vec<Vec<Json>>,
    next: Option<String>,
    update_type: Option<String>,
    update_count: Option<u64>,
    error: Option<DriverError>,
}

impl Session {
    fn header(&self, name: &str) -> String {
        format!("{}{name}", self.flavor.prefix())
    }

    fn headers(&self) -> Vec<(String, String)> {
        let mut headers = vec![
            (self.header("User"), self.user.clone()),
            (self.header("Source"), self.source.clone()),
        ];
        if self.flavor == Flavor::Trino {
            headers.push((
                self.header("Client-Capabilities"),
                "PATH,PARAMETRIC_DATETIME".into(),
            ));
        }
        if let Some(catalog) = &self.catalog {
            headers.push((self.header("Catalog"), catalog.clone()));
        }
        if let Some(schema) = &self.schema {
            headers.push((self.header("Schema"), schema.clone()));
        }
        if let Some(path) = &self.path {
            headers.push((self.header("Path"), path.clone()));
        }
        if !self.properties.is_empty() {
            let joined: Vec<String> = self
                .properties
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect();
            headers.push((self.header("Session"), joined.join(",")));
        }
        if !self.prepared.is_empty() {
            let joined: Vec<String> = self
                .prepared
                .iter()
                .map(|(name, sql)| format!("{name}={sql}"))
                .collect();
            headers.push((self.header("Prepared-Statement"), joined.join(",")));
        }
        headers.push((
            self.header("Transaction-Id"),
            self.transaction.clone().unwrap_or_else(|| "NONE".into()),
        ));
        headers
    }

    /// What an answer's headers say to change in the session.
    fn apply(&mut self, headers: &[(String, String)]) {
        let prefix = self.flavor.prefix().to_ascii_lowercase();
        for (name, value) in headers {
            let Some(name) = name.strip_prefix(&prefix) else {
                continue;
            };
            match name {
                "set-catalog" => self.catalog = Some(value.clone()),
                "set-schema" => self.schema = Some(value.clone()),
                "set-path" => self.path = Some(value.clone()),
                "set-session" => {
                    if let Some((key, val)) = value.split_once('=') {
                        self.properties
                            .insert(key.trim().to_string(), val.trim().to_string());
                    }
                }
                "clear-session" => {
                    self.properties.remove(value.trim());
                }
                "added-prepare" => {
                    if let Some((key, val)) = value.split_once('=') {
                        self.prepared
                            .insert(key.trim().to_string(), val.trim().to_string());
                    }
                }
                "deallocated-prepare" => {
                    self.prepared.remove(value.trim());
                }
                "started-transaction-id" => self.transaction = Some(value.clone()),
                "clear-transaction-id" => self.transaction = None,
                _ => {}
            }
        }
    }

    async fn read_page(
        &mut self,
        method: Method,
        path: &str,
        body: Option<String>,
    ) -> Result<Page> {
        let headers = self.headers();
        let response = self.client.send(method, path, &headers, body).await?;
        if response.status != StatusCode::OK {
            let text = self
                .client
                .endpoint()
                .scrub(String::from_utf8_lossy(&response.body).trim());
            return Err(DriverError::Connection(if text.is_empty() {
                format!("the server answered {}", response.status)
            } else {
                format!("the server answered {}: {text}", response.status)
            }));
        }
        self.apply(&response.headers);
        let json: Json = serde_json::from_slice(&response.body)
            .map_err(|e| DriverError::Other(format!("the server's answer is not JSON: {e}")))?;
        let columns = json.get("columns").and_then(Json::as_array).map(|columns| {
            columns
                .iter()
                .map(|c| {
                    (
                        c.get("name")
                            .and_then(Json::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        c.get("type")
                            .and_then(Json::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    )
                })
                .collect()
        });
        let data = match json.get("data") {
            Some(Json::Array(rows)) => rows
                .iter()
                .map(|row| row.as_array().cloned().unwrap_or_default())
                .collect(),
            _ => Vec::new(),
        };
        Ok(Page {
            columns,
            data,
            next: json
                .get("nextUri")
                .and_then(Json::as_str)
                .map(str::to_string),
            update_type: json
                .get("updateType")
                .and_then(Json::as_str)
                .map(str::to_string),
            update_count: json.get("updateCount").and_then(Json::as_u64),
            error: json.get("error").map(server_error),
        })
    }

    /// A whole result: what the driver's own queries read.
    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Json>>> {
        let mut page = self
            .read_page(Method::POST, "/v1/statement", Some(sql.to_string()))
            .await?;
        let mut rows = Vec::new();
        loop {
            if let Some(error) = page.error.take() {
                return Err(error);
            }
            rows.append(&mut page.data);
            let Some(next) = page.next.take() else {
                return Ok(rows);
            };
            let path = path_of(&next).ok_or_else(|| bad_uri(&next))?;
            page = self.read_page(Method::GET, &path, None).await?;
        }
    }
}

fn bad_uri(uri: &str) -> DriverError {
    DriverError::Other(format!(
        "the server sent an address that cannot be followed: {uri}"
    ))
}

/// The server's error, as a driver error. A cancelled query is [`DriverError::Cancelled`].
fn server_error(error: &Json) -> DriverError {
    let name = error
        .get("errorName")
        .and_then(Json::as_str)
        .unwrap_or_default();
    if name == "USER_CANCELED" {
        return DriverError::Cancelled;
    }
    let message = error
        .get("message")
        .and_then(Json::as_str)
        .unwrap_or("the server reported an error")
        .to_string();
    DriverError::Database {
        code: (!name.is_empty()).then(|| name.to_string()),
        message,
        detail: None,
        hint: None,
        position: None,
    }
}

/// Cancels: remembers the last statement sent when it was asked, so that statement and every
/// one queued before it stop (a queued one never starts), and none sent after it is touched.
struct Cancel {
    sent: AtomicU64,
    upto: watch::Sender<u64>,
}

struct TrinoCancel(Arc<Cancel>);

#[async_trait]
impl CancelHandle for TrinoCancel {
    async fn cancel(&self) -> Result<()> {
        let sent = self.0.sent.load(Ordering::SeqCst);
        self.0.upto.send_if_modified(|upto| {
            if *upto < sent {
                *upto = sent;
                true
            } else {
                false
            }
        });
        Ok(())
    }
}

enum Request {
    Execute {
        sql: String,
        batch_size: usize,
        seq: u64,
        events: mpsc::Sender<Result<ExecEvent>>,
        started: oneshot::Sender<Result<()>>,
    },
    Rows {
        sql: String,
        reply: oneshot::Sender<Result<Vec<Vec<Json>>>>,
    },
    Transaction {
        reply: oneshot::Sender<bool>,
    },
    Catalog {
        set: Option<String>,
        reply: oneshot::Sender<Option<String>>,
    },
}

async fn actor(
    mut session: Session,
    mut requests: mpsc::Receiver<Request>,
    cancel: watch::Receiver<u64>,
    closed: Arc<AtomicBool>,
) {
    while let Some(request) = requests.recv().await {
        match request {
            Request::Execute {
                sql,
                batch_size,
                seq,
                events,
                started,
            } => {
                execute(
                    &mut session,
                    cancel.clone(),
                    seq,
                    &sql,
                    batch_size,
                    events,
                    started,
                    &closed,
                )
                .await;
            }
            Request::Rows { sql, reply } => {
                let result = session.rows(&sql).await;
                if let Err(DriverError::Connection(_)) = &result {
                    closed.store(true, Ordering::SeqCst);
                }
                let _ = reply.send(result);
            }
            Request::Transaction { reply } => {
                let _ = reply.send(session.transaction.is_some());
            }
            Request::Catalog { set, reply } => {
                if let Some(catalog) = set {
                    session.catalog = Some(catalog);
                    session.schema = None;
                }
                let _ = reply.send(session.catalog.clone());
            }
        }
    }
}

/// Asks the server to cancel the query whose next page is at `next`, on a connection of its
/// own, and waits until the query has stopped or [`CANCEL_WAIT`] has passed.
async fn cancel_on_server(session: &mut Session, next: &str) {
    let Some(path) = path_of(next) else {
        return;
    };
    let mut alone = Client::new(session.client.endpoint().clone());
    let headers = session.headers();
    let _ = alone.send(Method::DELETE, &path, &headers, None).await;
    // The query's next page now says how it ended (cancelled, or finished before the cancel).
    let _ = tokio::time::timeout(CANCEL_WAIT, async {
        let mut path = path;
        for _ in 0..50 {
            match session.read_page(Method::GET, &path, None).await {
                Ok(page) => match page.next.as_deref().and_then(path_of) {
                    Some(next) if page.error.is_none() => path = next,
                    _ => return,
                },
                Err(_) => return,
            }
        }
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    session: &mut Session,
    mut cancel: watch::Receiver<u64>,
    seq: u64,
    sql: &str,
    batch_size: usize,
    events: mpsc::Sender<Result<ExecEvent>>,
    started: oneshot::Sender<Result<()>>,
    closed: &AtomicBool,
) {
    if *cancel.borrow() >= seq {
        let _ = started.send(Err(DriverError::Cancelled));
        return;
    }
    let first = session
        .read_page(Method::POST, "/v1/statement", Some(sql.to_string()))
        .await;
    let mut page = match first {
        Ok(mut page) => match page.error.take() {
            Some(error) => {
                let _ = started.send(Err(error));
                return;
            }
            None => page,
        },
        Err(e) => {
            if let DriverError::Connection(_) = e {
                closed.store(true, Ordering::SeqCst);
            }
            let _ = started.send(Err(e));
            return;
        }
    };
    let _ = started.send(Ok(()));

    let batch_size = batch_size.max(1);
    let mut batches: Option<Batches> = None;
    let mut update: Option<(String, Option<u64>)> = None;
    let outcome: Result<()> = loop {
        if let Some(kind) = page.update_type.take() {
            update = Some((
                kind,
                page.update_count.or(update.as_ref().and_then(|u| u.1)),
            ));
        }
        if update.is_none()
            && batches.is_none()
            && let Some(columns) = page.columns.take()
        {
            let schema = Arc::new(ResultSchema::new(
                columns
                    .into_iter()
                    .map(|(name, native)| types::column(name, native))
                    .collect(),
            ));
            if events
                .send(Ok(ExecEvent::Columns(schema.clone())))
                .await
                .is_err()
            {
                if let Some(next) = page.next.take() {
                    cancel_on_server(session, &next).await;
                }
                return;
            }
            batches = Some(Batches::new(schema));
        }
        if let Some(b) = batches.as_mut() {
            let mut failed = None;
            for row in &page.data {
                let full = match b.push(row) {
                    Ok(()) if b.rows() >= batch_size => b.take(),
                    Ok(()) => Ok(None),
                    Err(e) => Err(e),
                };
                match full {
                    Ok(Some(batch)) => {
                        if events.send(Ok(ExecEvent::Batch(batch))).await.is_err() {
                            if let Some(next) = page.next.take() {
                                cancel_on_server(session, &next).await;
                            }
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = failed {
                if let Some(next) = page.next.take() {
                    cancel_on_server(session, &next).await;
                }
                break Err(e);
            }
        }
        if let Some(error) = page.error.take() {
            break Err(error);
        }
        let Some(next) = page.next.take() else {
            break Ok(());
        };
        let Some(path) = path_of(&next) else {
            break Err(bad_uri(&next));
        };
        if *cancel.borrow() >= seq {
            cancel_on_server(session, &next).await;
            break Err(DriverError::Cancelled);
        }
        let read = {
            let fetch = session.read_page(Method::GET, &path, None);
            tokio::pin!(fetch);
            loop {
                tokio::select! {
                    read = &mut fetch => break Some(read),
                    changed = cancel.changed() => {
                        if changed.is_ok() && *cancel.borrow() >= seq {
                            break None;
                        }
                    }
                }
            }
        };
        match read {
            Some(Ok(next_page)) => page = next_page,
            Some(Err(e)) => break Err(e),
            None => {
                cancel_on_server(session, &next).await;
                break Err(DriverError::Cancelled);
            }
        }
    };
    match outcome {
        Ok(()) => {
            if let Some(b) = batches.as_mut() {
                match b.take() {
                    Ok(Some(batch)) => {
                        if events.send(Ok(ExecEvent::Batch(batch))).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let _ = events.send(Err(e)).await;
                        return;
                    }
                }
            }
            let rows_affected = update.and_then(|(_, count)| count);
            let _ = events.send(Ok(ExecEvent::Done { rows_affected })).await;
        }
        Err(e) => {
            if let DriverError::Connection(_) = e {
                closed.store(true, Ordering::SeqCst);
            }
            let _ = events.send(Err(e)).await;
        }
    }
}

struct TrinoConnection {
    capabilities: Capabilities,
    requests: mpsc::Sender<Request>,
    cancel: Arc<Cancel>,
    closed: Arc<AtomicBool>,
}

impl TrinoConnection {
    fn gone() -> DriverError {
        DriverError::Connection("the connection is closed".into())
    }

    /// One of the driver's own queries, as JSON rows.
    pub(crate) async fn rows(&self, sql: &str) -> Result<Vec<Vec<Json>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Rows {
                sql: sql.to_string(),
                reply,
            })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())?
    }

    async fn catalog(&self, set: Option<String>) -> Result<Option<String>> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Catalog { set, reply })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())
    }
}

#[async_trait]
impl Connection for TrinoConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let seq = self.cancel.sent.fetch_add(1, Ordering::SeqCst) + 1;
        let (events, receiver) = mpsc::channel(4);
        let (started, answer) = oneshot::channel();
        self.requests
            .send(Request::Execute {
                sql: sql.to_string(),
                batch_size: opts.batch_size,
                seq,
                events,
                started,
            })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())??;
        let mut receiver = receiver;
        Ok(futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)).boxed())
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(TrinoCancel(self.cancel.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        let rows = self
            .rows("SELECT node_version FROM system.runtime.nodes WHERE coordinator")
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_string())
    }

    async fn keep_alive(&self) -> Result<KeepAlive> {
        if self.transaction_state().await? == TransactionState::Open {
            return Ok(KeepAlive::InTransaction);
        }
        self.rows("SELECT 1").await?;
        Ok(KeepAlive::Alive)
    }

    /// Known without asking: the transaction id is the client's.
    async fn transaction_state(&self) -> Result<TransactionState> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Transaction { reply })
            .await
            .map_err(|_| Self::gone())?;
        Ok(if answer.await.map_err(|_| Self::gone())? {
            TransactionState::Open
        } else {
            TransactionState::Idle
        })
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// The catalog: Trino's first level, the explorer's databases.
    async fn current_database(&self) -> Result<Option<String>> {
        self.catalog(None).await
    }

    async fn database_context(&self) -> Result<Option<String>> {
        self.catalog(None).await
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        self.catalog(Some(database.to_string())).await.map(|_| ())
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        introspect::list_catalogs(self).await
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let catalog = self.catalog(None).await?;
        introspect::list_schemas(self, catalog.as_deref()).await
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let catalog = self.catalog(None).await?;
        introspect::list_objects(self, catalog.as_deref(), schema).await
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let catalog = self.catalog(None).await?;
        introspect::describe_table(self, catalog.as_deref(), schema, name).await
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        let catalog = self.catalog(None).await?;
        introspect::list_columns(self, catalog.as_deref(), schema).await
    }

    /// Neither has foreign keys.
    async fn list_referencing_keys(
        &self,
        _schema: &str,
        _name: &str,
    ) -> Result<Vec<ReferencingKey>> {
        Ok(Vec::new())
    }

    async fn relation_definition(&self, schema: &str, name: &str) -> Result<String> {
        let catalog = self.catalog(None).await?;
        introspect::relation_definition(self, catalog.as_deref(), schema, name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_strings_are_quoted() {
        assert_eq!(quote("a\"b"), "\"a\"\"b\"");
        assert_eq!(literal("it's"), "'it''s'");
    }

    #[test]
    fn errors_keep_their_name() {
        let error: Json = serde_json::from_str(
            r#"{"message":"line 1:15: Table 'x' does not exist","errorName":"TABLE_NOT_FOUND","errorCode":46}"#,
        )
        .unwrap();
        match server_error(&error) {
            DriverError::Database { code, message, .. } => {
                assert_eq!(code.as_deref(), Some("TABLE_NOT_FOUND"));
                assert!(message.contains("does not exist"));
            }
            other => panic!("{other:?}"),
        }
        let cancelled: Json =
            serde_json::from_str(r#"{"message":"x","errorName":"USER_CANCELED"}"#).unwrap();
        assert!(matches!(server_error(&cancelled), DriverError::Cancelled));
    }
}
