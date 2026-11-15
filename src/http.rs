//! Trino's and Presto's HTTP protocol, over connections this driver opens itself.
//!
//! Every request goes to the address SpelaionDB gave (`connect_addr`, the local end of a tunnel
//! it opened) or to `host:port`, and never through a proxy: the core decides the route. TLS is
//! verified against `host`, which is also the name sent in SNI and in `Host`. The server's
//! `nextUri` names the server as it sees itself; only its path is followed, on this route.
//!
//! A password travels only over TLS (in `Authorization`), as Trino and Presto require.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use spelaion_driver_api::{DriverError, Result, Secret};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::tls::Tls;

/// A connection idle longer than this is not used again: a request sent on a connection the
/// server is closing would be lost on the way.
const REUSE_WITHIN: Duration = Duration::from_secs(3);

/// The most of one response that is read: a page of results, or an error.
const MAX_BODY: usize = 256 * 1024 * 1024;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Where the server is and how to reach it.
pub struct Endpoint {
    host: String,
    port: u16,
    connect_addr: Option<SocketAddr>,
    tls: Tls,
    /// `prefer` once decided: 0 not yet, 1 TLS, 2 plain.
    preferred: AtomicU8,
    connect_timeout: Duration,
    /// `Basic …` for a password, sent only over TLS.
    authorization: Option<String>,
    password: Option<Secret>,
}

/// One response: its status, its headers (names in lower case) and its whole body.
pub struct Response {
    pub status: StatusCode,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Endpoint {
    pub fn new(
        host: String,
        port: u16,
        connect_addr: Option<SocketAddr>,
        tls: Tls,
        connect_timeout: Duration,
        user: &str,
        password: Option<Secret>,
    ) -> Self {
        let authorization = password
            .as_ref()
            .filter(|p| !p.expose().is_empty())
            .map(|p| {
                format!(
                    "Basic {}",
                    base64(format!("{user}:{}", p.expose()).as_bytes())
                )
            });
        Self {
            host,
            port,
            connect_addr,
            tls,
            preferred: AtomicU8::new(0),
            connect_timeout,
            authorization,
            password,
        }
    }

    /// The password out of anything shown.
    pub fn scrub(&self, text: &str) -> String {
        match &self.password {
            Some(password) if !password.expose().is_empty() => {
                text.replace(password.expose(), "***")
            }
            _ => text.to_string(),
        }
    }

    async fn tcp(&self) -> Result<TcpStream> {
        let stream = match self.connect_addr {
            Some(addr) => TcpStream::connect(addr).await,
            None => TcpStream::connect((self.host.as_str(), self.port)).await,
        };
        let stream = stream.map_err(|e| {
            DriverError::Connection(format!("could not reach {}:{}: {e}", self.host, self.port))
        })?;
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }

    async fn tls_stream(
        &self,
        config: &Arc<rustls::ClientConfig>,
        tcp: TcpStream,
    ) -> std::io::Result<Box<dyn Io>> {
        let name = ServerName::try_from(self.host.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let stream = TlsConnector::from(config.clone())
            .connect(name, tcp)
            .await?;
        Ok(Box::new(stream))
    }

    /// A new connection, with the TLS its mode asks for, and whether it is encrypted.
    async fn open(&self) -> Result<(Box<dyn Io>, bool)> {
        let tcp = self.tcp().await?;
        let tls_error = |e: std::io::Error| DriverError::Connection(format!("TLS: {e}"));
        match &self.tls {
            Tls::Off => Ok((Box::new(tcp), false)),
            Tls::Required(config) => {
                Ok((self.tls_stream(config, tcp).await.map_err(tls_error)?, true))
            }
            Tls::Preferred(config) => match self.preferred.load(Ordering::SeqCst) {
                1 => Ok((self.tls_stream(config, tcp).await.map_err(tls_error)?, true)),
                2 => Ok((Box::new(tcp), false)),
                _ => match self.tls_stream(config, tcp).await {
                    Ok(stream) => {
                        self.preferred.store(1, Ordering::SeqCst);
                        Ok((stream, true))
                    }
                    Err(_) => {
                        self.preferred.store(2, Ordering::SeqCst);
                        Ok((Box::new(self.tcp().await?), false))
                    }
                },
            },
        }
    }

    async fn handshake(&self) -> Result<(SendRequest<Full<Bytes>>, bool)> {
        let open = async {
            let (io, encrypted) = self.open().await?;
            if self.authorization.is_some() && !encrypted {
                return Err(DriverError::Connection(
                    "a password is only sent over TLS: this connection is not encrypted".into(),
                ));
            }
            let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
                .await
                .map_err(|e| DriverError::Connection(format!("HTTP: {e}")))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok((sender, encrypted))
        };
        match tokio::time::timeout(self.connect_timeout, open).await {
            Ok(result) => result,
            Err(_) => Err(DriverError::Connection(format!(
                "could not connect to {}:{} within {} s",
                self.host,
                self.port,
                self.connect_timeout.as_secs()
            ))),
        }
    }

    fn request(
        &self,
        method: Method,
        path: &str,
        headers: &[(String, String)],
        body: Option<String>,
    ) -> Result<Request<Full<Bytes>>> {
        let host = if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        };
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::HOST, host);
        for (name, value) in headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if let Some(authorization) = &self.authorization {
            builder = builder.header(hyper::header::AUTHORIZATION, authorization.as_str());
        }
        if body.is_some() {
            builder = builder.header(hyper::header::CONTENT_TYPE, "text/plain; charset=UTF-8");
        }
        builder
            .body(Full::new(Bytes::from(body.unwrap_or_default())))
            .map_err(|e| DriverError::Other(format!("HTTP request: {e}")))
    }
}

/// One connection to the server, opened again when the last one is too old or closed.
pub struct Client {
    endpoint: Arc<Endpoint>,
    open: Option<(SendRequest<Full<Bytes>>, Instant)>,
}

impl Client {
    pub fn new(endpoint: Arc<Endpoint>) -> Self {
        Self {
            endpoint,
            open: None,
        }
    }

    pub fn endpoint(&self) -> &Arc<Endpoint> {
        &self.endpoint
    }

    /// Sends one request and reads its whole answer. A request is sent once: if it is lost on
    /// the way, it is not sent again.
    pub async fn send(
        &mut self,
        method: Method,
        path: &str,
        headers: &[(String, String)],
        body: Option<String>,
    ) -> Result<Response> {
        let request = self.endpoint.request(method, path, headers, body)?;
        let reusable = match &mut self.open {
            Some((sender, used)) if used.elapsed() < REUSE_WITHIN && !sender.is_closed() => {
                sender.ready().await.is_ok()
            }
            _ => false,
        };
        if !reusable {
            let (sender, _) = self.endpoint.handshake().await?;
            self.open = Some((sender, Instant::now()));
        }
        let (sender, used) = self.open.as_mut().expect("just opened");
        let sent = sender.send_request(request).await;
        *used = Instant::now();
        let response = match sent {
            Ok(response) => response,
            Err(e) => {
                self.open = None;
                return Err(DriverError::Connection(
                    self.endpoint.scrub(&format!("the request was lost: {e}")),
                ));
            }
        };
        let status = response.status();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                Some((
                    name.as_str().to_ascii_lowercase(),
                    value.to_str().ok()?.to_string(),
                ))
            })
            .collect();
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        loop {
            match body.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        bytes.extend_from_slice(&data);
                        if bytes.len() > MAX_BODY {
                            return Err(DriverError::Other(
                                "the server's answer is too large".into(),
                            ));
                        }
                    }
                }
                Some(Err(e)) => {
                    self.open = None;
                    return Err(DriverError::Connection(format!(
                        "the answer was cut off: {e}"
                    )));
                }
                None => break,
            }
        }
        Ok(Response {
            status,
            headers,
            body: bytes,
        })
    }
}

/// The path and query of `uri` (`http://host:8080/v1/statement/…` → `/v1/statement/…`): what is
/// followed on this connection's route, whatever host the server names.
pub fn path_of(uri: &str) -> Option<String> {
    let rest = uri
        .strip_prefix("http://")
        .or_else(|| uri.strip_prefix("https://"))?;
    let slash = rest.find('/')?;
    Some(rest[slash..].to_string())
}

/// `application/x-www-form-urlencoded`'s encoding, for header values the protocol encodes.
pub fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(BASE64[((n >> shift) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Standard base64 back to bytes; `None` for anything that is not.
pub fn unbase64(text: &str) -> Option<Vec<u8>> {
    let text = text.trim_end_matches('=');
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for c in text.bytes() {
        let v = BASE64.iter().position(|&b| b == c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_uris_are_followed_by_their_path() {
        assert_eq!(
            path_of("http://trino:8080/v1/statement/queued/q1/y/1").as_deref(),
            Some("/v1/statement/queued/q1/y/1")
        );
        assert_eq!(path_of("https://h/x?y=1").as_deref(), Some("/x?y=1"));
        assert_eq!(path_of("/relative"), None);
    }

    #[test]
    fn base64_round_trips() {
        for bytes in [&b""[..], b"f", b"fo", b"foo", b"foob", b"user:p\xc3\xa4ss"] {
            assert_eq!(unbase64(&base64(bytes)).as_deref(), Some(bytes));
        }
        assert_eq!(base64(b"user:pass"), "dXNlcjpwYXNz");
        assert_eq!(unbase64("not base64!"), None);
    }

    #[test]
    fn header_values_are_encoded() {
        assert_eq!(encode("a b,c=d"), "a%20b%2Cc%3Dd");
    }
}
