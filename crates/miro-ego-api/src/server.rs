//! Async `tokio` + `hyper` + `rustls` (TLS 1.2/1.3) `GatewayServer` with bounded TLS handshake
//! timeout, bounded connection concurrency, and graceful shutdown.

use crate::backend::GatewayBackend;
use crate::error::{ApiError, ApiResult};
use crate::middleware::{DEFAULT_REQUEST_BODY_TIMEOUT, GatewayRuntimeConfig, GatewayState};
use crate::router::build_router;
use crate::security::{SecurityPolicy, normalize_allowed_hosts};
use axum::Router;
use futures_util::task::AtomicWaker;
use hyper::body::{Body as HttpBody, Frame, Incoming, SizeHint};
use hyper::server::conn::http1;
use hyper::service::Service as HyperService;
use hyper_util::rt::{TokioIo, TokioTimer};
use rustls::ServerConfig as RustlsServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::convert::Infallible;
use std::future::Future;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;

/// TLS 1.2+ server configuration using `rustls` with the pure-Rust/C `ring` crypto provider.
#[derive(Clone)]
pub struct TlsConfig {
    server_config: Arc<RustlsServerConfig>,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig").finish_non_exhaustive()
    }
}

impl TlsConfig {
    /// Construct a TLS 1.2 / 1.3 `TlsConfig` from PEM-encoded certificate chain and private key bytes.
    pub fn from_pem_bytes(cert_pem: &[u8], key_pem: &[u8]) -> ApiResult<Self> {
        let mut cert_reader = BufReader::new(cert_pem);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| ApiError::bad_request(format!("invalid TLS certificate PEM: {err}")))?;
        if certs.is_empty() {
            return Err(ApiError::bad_request(
                "TLS certificate PEM contains no certificates",
            ));
        }

        let mut key_reader = BufReader::new(key_pem);
        let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|err| ApiError::bad_request(format!("invalid TLS private key PEM: {err}")))?
            .ok_or_else(|| ApiError::bad_request("TLS private key PEM contains no private key"))?;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = RustlsServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|err| ApiError::internal("tls_config_error", format!("{err}")))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| ApiError::bad_request(format!("TLS certificate/key mismatch: {err}")))?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];

        Ok(Self {
            server_config: Arc::new(config),
        })
    }

    /// Construct a TLS 1.2 / 1.3 `TlsConfig` from PEM files on disk.
    pub fn from_pem_files(cert_path: &Path, key_path: &Path) -> ApiResult<Self> {
        let cert_pem = std::fs::read(cert_path)
            .map_err(|err| ApiError::bad_request(format!("read TLS cert file: {err}")))?;
        let key_pem = std::fs::read(key_path)
            .map_err(|err| ApiError::bad_request(format!("read TLS key file: {err}")))?;
        Self::from_pem_bytes(&cert_pem, &key_pem)
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(Arc::clone(&self.server_config))
    }
}

/// Complete configuration for [`GatewayServer`].
#[derive(Debug, Clone)]
pub struct GatewayServerConfig {
    pub bind_addr: SocketAddr,
    pub allowed_hosts: Vec<String>,
    pub tls: Option<TlsConfig>,
    pub tls_handshake_timeout: Duration,
    pub request_header_timeout: Duration,
    pub request_body_timeout: Duration,
    pub keep_alive_idle_timeout: Duration,
    pub max_connections: usize,
    pub runtime: GatewayRuntimeConfig,
    pub shutdown_drain_timeout: Duration,
}

impl GatewayServerConfig {
    pub fn http(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            allowed_hosts: Vec::new(),
            tls: None,
            tls_handshake_timeout: Duration::from_secs(10),
            request_header_timeout: Duration::from_secs(30),
            request_body_timeout: DEFAULT_REQUEST_BODY_TIMEOUT,
            keep_alive_idle_timeout: Duration::from_secs(15),
            max_connections: 64,
            runtime: GatewayRuntimeConfig::default(),
            shutdown_drain_timeout: Duration::from_secs(5),
        }
    }

    pub fn https(bind_addr: SocketAddr, tls: TlsConfig) -> Self {
        let mut cfg = Self::http(bind_addr);
        cfg.tls = Some(tls);
        cfg.runtime.external_scheme = "https".to_owned();
        cfg
    }
}

/// Bound async HTTP / HTTPS gateway server ready to serve connections until a shutdown signal.
pub struct GatewayServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    router: Router,
    state: Arc<GatewayState>,
    tls_acceptor: Option<TlsAcceptor>,
    tls_handshake_timeout: Duration,
    request_header_timeout: Duration,
    keep_alive_idle_timeout: Duration,
    max_connections: usize,
    shutdown_drain_timeout: Duration,
}

impl GatewayServer {
    /// Bind the TCP listener and assemble the complete Miro-Ego v1 router and TLS acceptor.
    pub async fn bind(
        config: GatewayServerConfig,
        backend: Arc<dyn GatewayBackend>,
        security: Arc<SecurityPolicy>,
    ) -> ApiResult<Self> {
        let listener = TcpListener::bind(config.bind_addr).await.map_err(|err| {
            ApiError::internal("bind_failed", format!("bind {}: {err}", config.bind_addr))
        })?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| ApiError::internal("bind_failed", format!("local_addr: {err}")))?;

        let listen_host = local_addr.ip().to_string();
        let allowed_hosts = normalize_allowed_hosts(&listen_host, &config.allowed_hosts);
        let mut runtime = config.runtime;
        if config.tls.is_some() {
            runtime.external_scheme = "https".to_owned();
        }
        if config.request_body_timeout != DEFAULT_REQUEST_BODY_TIMEOUT
            || runtime.request_body_timeout == DEFAULT_REQUEST_BODY_TIMEOUT
        {
            runtime.request_body_timeout =
                config.request_body_timeout.max(Duration::from_millis(20));
        }
        let state = GatewayState::new(backend, security, allowed_hosts, runtime);
        let router = build_router(Arc::clone(&state));
        let tls_acceptor = config.tls.as_ref().map(TlsConfig::acceptor);

        Ok(Self {
            listener,
            local_addr,
            router,
            state,
            tls_acceptor,
            tls_handshake_timeout: config.tls_handshake_timeout.max(Duration::from_millis(50)),
            request_header_timeout: config
                .request_header_timeout
                .max(Duration::from_millis(100)),
            keep_alive_idle_timeout: config
                .keep_alive_idle_timeout
                .max(Duration::from_millis(50)),
            max_connections: config.max_connections.max(1),
            shutdown_drain_timeout: config
                .shutdown_drain_timeout
                .max(Duration::from_millis(100)),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn is_tls(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    pub fn state(&self) -> Arc<GatewayState> {
        Arc::clone(&self.state)
    }

    /// Serve incoming HTTP / HTTPS connections until `shutdown` resolves, then gracefully drain.
    pub async fn serve_with_graceful_shutdown<F>(self, shutdown: F) -> ApiResult<()>
    where
        F: Future<Output = ()> + Send,
    {
        let conn_semaphore = Arc::new(Semaphore::new(self.max_connections));
        let shutdown_rx = self.state.subscribe_shutdown();
        let mut join_set = tokio::task::JoinSet::new();
        let mut shutdown = pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    self.state.signal_shutdown();
                    break;
                }
                accepted = self.listener.accept() => {
                    let (stream, _peer_addr) = match accepted {
                        Ok(pair) => pair,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            continue;
                        }
                    };
                    let _ = stream.set_nodelay(true);

                    // Enforce strict bounded max concurrent connections without starving accept loop.
                    let Ok(permit) = Arc::clone(&conn_semaphore).try_acquire_owned() else {
                        drop(stream);
                        continue;
                    };

                    let router = self.router.clone();
                    let tls_acceptor = self.tls_acceptor.clone();
                    let tls_timeout = self.tls_handshake_timeout;
                    let header_timeout = self.request_header_timeout;
                    let idle_timeout = self.keep_alive_idle_timeout;
                    let drain_timeout = self.shutdown_drain_timeout;
                    let mut conn_shutdown_rx = shutdown_rx.clone();

                    join_set.spawn(async move {
                        let _permit = permit;
                        if let Some(acceptor) = tls_acceptor {
                            let handshake = tokio::select! {
                                biased;
                                _ = wait_for_shutdown(&mut conn_shutdown_rx) => return,
                                res = tokio::time::timeout(tls_timeout, acceptor.accept(stream)) => res,
                            };
                            let Ok(Ok(tls_stream)) = handshake else {
                                return;
                            };
                            serve_http1_stream(
                                tls_stream,
                                router,
                                header_timeout,
                                idle_timeout,
                                drain_timeout,
                                &mut conn_shutdown_rx,
                            )
                            .await;
                        } else {
                            serve_http1_stream(
                                stream,
                                router,
                                header_timeout,
                                idle_timeout,
                                drain_timeout,
                                &mut conn_shutdown_rx,
                            )
                            .await;
                        }
                    });

                    // Reap completed connection handles opportunistically.
                    while join_set.try_join_next().is_some() {}
                }
            }
        }

        // Wait up to shutdown_drain_timeout for active connections to close cleanly, then abort stragglers.
        let drain = async { while join_set.join_next().await.is_some() {} };
        if tokio::time::timeout(self.shutdown_drain_timeout, drain)
            .await
            .is_err()
        {
            join_set.abort_all();
            while join_set.join_next().await.is_some() {}
        }
        Ok(())
    }
}

struct ConnectionIdleTracker {
    active_count: AtomicUsize,
    idle_epoch: AtomicU64,
    read_waker: AtomicWaker,
}

impl ConnectionIdleTracker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            active_count: AtomicUsize::new(0),
            idle_epoch: AtomicU64::new(0),
            read_waker: AtomicWaker::new(),
        })
    }
}

struct ActiveRequestGuard {
    tracker: Arc<ConnectionIdleTracker>,
}

impl ActiveRequestGuard {
    fn acquire(tracker: Arc<ConnectionIdleTracker>) -> Self {
        tracker.active_count.fetch_add(1, Ordering::AcqRel);
        Self { tracker }
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        let prev = self.tracker.active_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.tracker.idle_epoch.fetch_add(1, Ordering::AcqRel);
            self.tracker.read_waker.wake();
        }
    }
}

struct GuardedResponseBody {
    inner: axum::body::Body,
    guard: Option<ActiveRequestGuard>,
}

impl HttpBody for GuardedResponseBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let poll = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(poll, Poll::Ready(None)) {
            self.guard.take();
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Clone)]
struct TrackedRouterService {
    router: Router,
    tracker: Arc<ConnectionIdleTracker>,
}

impl HyperService<hyper::Request<Incoming>> for TrackedRouterService {
    type Response = hyper::Response<GuardedResponseBody>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn call(&self, req: hyper::Request<Incoming>) -> Self::Future {
        let guard = ActiveRequestGuard::acquire(Arc::clone(&self.tracker));
        let router = self.router.clone();
        Box::pin(async move {
            let Ok(resp) = router.oneshot(req).await;
            Ok(resp.map(|body| GuardedResponseBody {
                inner: body,
                guard: Some(guard),
            }))
        })
    }
}

struct IdleTimeoutIo<IO> {
    io: IO,
    idle_timeout: Duration,
    tracker: Arc<ConnectionIdleTracker>,
    observed_epoch: u64,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl<IO> IdleTimeoutIo<IO> {
    fn new(io: IO, idle_timeout: Duration, tracker: Arc<ConnectionIdleTracker>) -> Self {
        Self {
            io,
            idle_timeout,
            tracker,
            observed_epoch: 0,
            sleep: Box::pin(tokio::time::sleep(idle_timeout)),
        }
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for IdleTimeoutIo<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        let before = buf.filled().len();
        match Pin::new(&mut this.io).poll_read(cx, buf) {
            Poll::Ready(res) => {
                if res.is_ok() && buf.filled().len() > before {
                    let ep = this.tracker.idle_epoch.fetch_add(1, Ordering::AcqRel) + 1;
                    this.observed_epoch = ep;
                    this.sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + this.idle_timeout);
                }
                Poll::Ready(res)
            }
            Poll::Pending => {
                if this.tracker.active_count.load(Ordering::Acquire) > 0 {
                    this.tracker.read_waker.register(cx.waker());
                    if this.tracker.active_count.load(Ordering::Acquire) > 0 {
                        return Poll::Pending;
                    }
                }
                let cur_epoch = this.tracker.idle_epoch.load(Ordering::Acquire);
                if cur_epoch != this.observed_epoch {
                    this.observed_epoch = cur_epoch;
                    this.sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + this.idle_timeout);
                }
                if this.sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "HTTP/1.1 keep-alive idle timeout",
                    )));
                }
                Poll::Pending
            }
        }
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for IdleTimeoutIo<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;
        let poll = Pin::new(&mut this.io).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = poll
            && n > 0
        {
            let ep = this.tracker.idle_epoch.fetch_add(1, Ordering::AcqRel) + 1;
            this.observed_epoch = ep;
            this.sleep
                .as_mut()
                .reset(tokio::time::Instant::now() + this.idle_timeout);
        }
        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

async fn serve_http1_stream<IO>(
    io: IO,
    router: Router,
    header_timeout: Duration,
    idle_timeout: Duration,
    drain_timeout: Duration,
    shutdown_rx: &mut watch::Receiver<bool>,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let tracker = ConnectionIdleTracker::new();
    let tracked_io = IdleTimeoutIo::new(io, idle_timeout, Arc::clone(&tracker));
    let service = TrackedRouterService { router, tracker };
    let mut builder = http1::Builder::new();
    builder.timer(TokioTimer::new());
    builder.header_read_timeout(header_timeout);
    builder.keep_alive(true);
    let conn = builder.serve_connection(TokioIo::new(tracked_io), service);
    let mut conn = pin!(conn);

    tokio::select! {
        _ = &mut conn => {}
        _ = wait_for_shutdown(shutdown_rx) => {
            conn.as_mut().graceful_shutdown();
            let _ = tokio::time::timeout(drain_timeout, conn).await;
        }
    }
}

async fn wait_for_shutdown(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
}
