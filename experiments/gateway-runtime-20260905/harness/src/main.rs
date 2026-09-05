use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type AnyError = Box<dyn Error + Send + Sync>;

const HEADER_LIMIT: usize = 16 * 1024;
const PERMIT_WAIT: Duration = Duration::from_millis(200);
const PEER_POLL: Duration = Duration::from_millis(10);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug)]
struct Arguments {
    listen: SocketAddr,
    capacity: usize,
    certificate: Option<PathBuf>,
    private_key: Option<PathBuf>,
    python: PathBuf,
    python_fake: PathBuf,
}

impl Arguments {
    fn parse() -> Result<Self, AnyError> {
        let mut listen = "127.0.0.1:0".parse()?;
        let mut capacity = 1_usize;
        let mut certificate = None;
        let mut private_key = None;
        let mut python = PathBuf::from("python3");
        let mut python_fake = None;
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            let value = arguments
                .next()
                .ok_or_else(|| format!("missing value for {argument}"))?;
            match argument.as_str() {
                "--listen" => listen = value.parse()?,
                "--capacity" => capacity = value.parse()?,
                "--cert" => certificate = Some(PathBuf::from(value)),
                "--key" => private_key = Some(PathBuf::from(value)),
                "--python" => python = PathBuf::from(value),
                "--python-fake" => python_fake = Some(PathBuf::from(value)),
                _ => return Err(format!("unknown argument: {argument}").into()),
            }
        }
        if capacity == 0 {
            return Err("capacity must be greater than zero".into());
        }
        if certificate.is_some() != private_key.is_some() {
            return Err("--cert and --key must be supplied together".into());
        }
        Ok(Self {
            listen,
            capacity,
            certificate,
            private_key,
            python,
            python_fake: python_fake.ok_or("--python-fake is required")?,
        })
    }
}

#[derive(Debug)]
struct PoolState {
    active: usize,
}

#[derive(Debug)]
struct PermitPool {
    capacity: usize,
    state: Mutex<PoolState>,
    available: Condvar,
    acquired: AtomicU64,
    released: AtomicU64,
    rejected: AtomicU64,
    close_reasons: Mutex<BTreeMap<&'static str, u64>>,
}

impl PermitPool {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            state: Mutex::new(PoolState { active: 0 }),
            available: Condvar::new(),
            acquired: AtomicU64::new(0),
            released: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            close_reasons: Mutex::new(BTreeMap::new()),
        })
    }

    fn acquire(self: &Arc<Self>, timeout: Duration) -> Option<Permit> {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().expect("permit pool poisoned");
        while state.active >= self.capacity {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            let (next, result) = self
                .available
                .wait_timeout(state, remaining)
                .expect("permit pool poisoned while waiting");
            state = next;
            if result.timed_out() && state.active >= self.capacity {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }
        state.active += 1;
        self.acquired.fetch_add(1, Ordering::Relaxed);
        Some(Permit {
            pool: Arc::clone(self),
        })
    }

    fn record_close(&self, reason: &'static str) {
        *self
            .close_reasons
            .lock()
            .expect("close reason metrics poisoned")
            .entry(reason)
            .or_default() += 1;
    }

    fn active(&self) -> usize {
        self.state.lock().expect("permit pool poisoned").active
    }

    fn metrics_json(&self) -> String {
        let reasons = self
            .close_reasons
            .lock()
            .expect("close reason metrics poisoned")
            .iter()
            .map(|(reason, count)| format!("\"{reason}\":{count}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"active\":{},\"capacity\":{},\"acquired\":{},\"released\":{},\"rejected\":{},\"close_reasons\":{{{reasons}}}}}",
            self.active(),
            self.capacity,
            self.acquired.load(Ordering::Relaxed),
            self.released.load(Ordering::Relaxed),
            self.rejected.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug)]
struct Permit {
    pool: Arc<PermitPool>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.pool.state.lock().expect("permit pool poisoned");
        debug_assert!(state.active > 0);
        state.active -= 1;
        self.pool.released.fetch_add(1, Ordering::Relaxed);
        self.pool.available.notify_all();
    }
}

#[derive(Debug)]
struct ControlRequest {
    delay_ms: u64,
    reply: SyncSender<Result<String, String>>,
}

#[derive(Debug)]
struct RuntimeState {
    preview_pool: Arc<PermitPool>,
    sse_pool: Arc<PermitPool>,
    shutdown: AtomicBool,
    next_stream_id: AtomicU64,
    streams: Mutex<HashMap<u64, TcpStream>>,
    control: SyncSender<ControlRequest>,
    accepted_connections: AtomicU64,
    completed_connections: AtomicU64,
}

impl RuntimeState {
    fn new(capacity: usize, control: SyncSender<ControlRequest>) -> Arc<Self> {
        Arc::new(Self {
            preview_pool: PermitPool::new(capacity),
            sse_pool: PermitPool::new(capacity),
            shutdown: AtomicBool::new(false),
            next_stream_id: AtomicU64::new(1),
            streams: Mutex::new(HashMap::new()),
            control,
            accepted_connections: AtomicU64::new(0),
            completed_connections: AtomicU64::new(0),
        })
    }

    fn initiate_shutdown(&self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        let streams = self.streams.lock().expect("stream registry poisoned");
        for stream in streams.values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn metrics_json(&self) -> String {
        format!(
            "{{\"preview\":{},\"sse\":{},\"registered_streams\":{},\"accepted_connections\":{},\"completed_connections\":{}}}",
            self.preview_pool.metrics_json(),
            self.sse_pool.metrics_json(),
            self.streams.lock().expect("stream registry poisoned").len(),
            self.accepted_connections.load(Ordering::Relaxed),
            self.completed_connections.load(Ordering::Relaxed),
        )
    }
}

struct StreamSession {
    state: Arc<RuntimeState>,
    id: u64,
    permit: Option<Permit>,
    pool: Arc<PermitPool>,
    close_reason: Option<&'static str>,
}

impl StreamSession {
    fn new(
        state: Arc<RuntimeState>,
        pool: Arc<PermitPool>,
        permit: Permit,
        wake_stream: TcpStream,
    ) -> Self {
        let id = state.next_stream_id.fetch_add(1, Ordering::Relaxed);
        state
            .streams
            .lock()
            .expect("stream registry poisoned")
            .insert(id, wake_stream);
        Self {
            state,
            id,
            permit: Some(permit),
            pool,
            close_reason: None,
        }
    }

    fn close(&mut self, reason: &'static str) {
        if self.close_reason.is_none() {
            self.close_reason = Some(reason);
            self.pool.record_close(reason);
        }
    }
}

impl Drop for StreamSession {
    fn drop(&mut self) {
        if self.close_reason.is_none() {
            self.pool.record_close("handler_error");
        }
        self.state
            .streams
            .lock()
            .expect("stream registry poisoned")
            .remove(&self.id);
        self.permit.take();
    }
}

enum Connection {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl Read for Connection {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for Connection {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

#[derive(Debug)]
struct Request {
    target: String,
    headers: HashMap<String, String>,
}

impl Request {
    fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or(&self.target)
    }

    fn query_u64(&self, name: &str) -> Option<u64> {
        self.target.split_once('?').and_then(|(_, query)| {
            query.split('&').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                (key == name).then(|| value.parse().ok()).flatten()
            })
        })
    }
}

fn read_request(connection: &mut Connection) -> io::Result<Request> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 2048];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = connection.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed before request headers",
            ));
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > HEADER_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers exceed experiment bound",
            ));
        }
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "headers are not UTF-8"))?;
    let mut lines = text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let mut parts = request_line.split_whitespace();
    if parts.next() != Some("GET") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "only GET is supported",
        ));
    }
    let target = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing target"))?
        .to_owned();
    let mut headers = HashMap::new();
    for line in lines.take_while(|line| !line.is_empty()) {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    Ok(Request { target, headers })
}

fn response(
    connection: &mut Connection,
    status: &str,
    headers: &str,
    body: &[u8],
) -> io::Result<()> {
    write!(
        connection,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n",
        body.len()
    )?;
    connection.write_all(body)?;
    connection.flush()
}

fn wait_for_disconnect(connection: &mut Connection, state: &RuntimeState) -> &'static str {
    let mut buffer = [0_u8; 1];
    loop {
        if state.shutdown.load(Ordering::Acquire) {
            return "server_shutdown";
        }
        match connection.read(&mut buffer) {
            Ok(0) => return "peer_disconnect",
            Ok(_) => continue,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) => {}
            Err(_) => return "peer_disconnect",
        }
    }
}

fn sse_payload(cursor: Option<u64>) -> Vec<u8> {
    let first = cursor.map_or(1, |value| value.saturating_add(1).clamp(1, 3));
    (first..=3)
        .map(|id| format!("id: {id}\nevent: snapshot\ndata: {{\"revision\":{id}}}\n\n"))
        .collect::<String>()
        .into_bytes()
}

fn handle_stream(
    connection: &mut Connection,
    wake_stream: TcpStream,
    state: Arc<RuntimeState>,
    pool: Arc<PermitPool>,
    initial_body: &[u8],
    content_type: &str,
) -> io::Result<()> {
    let Some(permit) = pool.acquire(PERMIT_WAIT) else {
        return response(
            connection,
            "503 Service Unavailable",
            "Retry-After: 1\r\nContent-Type: application/json\r\n",
            b"{\"error\":\"capacity_exhausted\"}",
        );
    };
    let mut session = StreamSession::new(state, pool, permit, wake_stream);
    write!(
        connection,
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"
    )?;
    connection.write_all(initial_body)?;
    connection.flush()?;
    let reason = wait_for_disconnect(connection, &session.state);
    session.close(reason);
    Ok(())
}

fn handle_slow_stream(
    connection: &mut Connection,
    wake_stream: TcpStream,
    state: Arc<RuntimeState>,
) -> io::Result<()> {
    let pool = Arc::clone(&state.preview_pool);
    let Some(permit) = pool.acquire(PERMIT_WAIT) else {
        return response(
            connection,
            "503 Service Unavailable",
            "Retry-After: 1\r\n",
            b"capacity exhausted",
        );
    };
    let mut session = StreamSession::new(state, pool, permit, wake_stream);
    connection.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
    )?;
    let chunk = [b'x'; 64 * 1024];
    loop {
        if session.state.shutdown.load(Ordering::Acquire) {
            session.close("server_shutdown");
            return Ok(());
        }
        if connection.write_all(&chunk).is_err() || connection.flush().is_err() {
            session.close("peer_disconnect");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn handle_control(
    connection: &mut Connection,
    request: &Request,
    state: &RuntimeState,
) -> io::Result<()> {
    let delay_ms = request.query_u64("delay_ms").unwrap_or(0).min(5_000);
    let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
    let command = ControlRequest {
        delay_ms,
        reply: reply_sender,
    };
    match state.control.try_send(command) {
        Ok(()) => match reply_receiver.recv_timeout(CONTROL_TIMEOUT) {
            Ok(Ok(body)) => response(
                connection,
                "200 OK",
                "Content-Type: application/json\r\n",
                body.as_bytes(),
            ),
            Ok(Err(error)) => response(
                connection,
                "502 Bad Gateway",
                "Content-Type: text/plain\r\n",
                error.as_bytes(),
            ),
            Err(_) => response(
                connection,
                "504 Gateway Timeout",
                "Content-Type: text/plain\r\n",
                b"control fake timeout",
            ),
        },
        Err(TrySendError::Full(_)) => response(
            connection,
            "503 Service Unavailable",
            "Retry-After: 1\r\nContent-Type: text/plain\r\n",
            b"control queue full",
        ),
        Err(TrySendError::Disconnected(_)) => response(
            connection,
            "503 Service Unavailable",
            "Retry-After: 1\r\nContent-Type: text/plain\r\n",
            b"control worker unavailable",
        ),
    }
}

fn handle_connection(
    stream: TcpStream,
    tls: Option<Arc<ServerConfig>>,
    state: Arc<RuntimeState>,
) -> Result<(), AnyError> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let wake_stream = stream.try_clone()?;
    let mut connection = match tls {
        Some(config) => Connection::Tls(Box::new(StreamOwned::new(
            ServerConnection::new(config)?,
            stream,
        ))),
        None => Connection::Plain(stream),
    };
    let request = read_request(&mut connection)?;
    wake_stream.set_read_timeout(Some(PEER_POLL))?;
    match request.path() {
        "/health" | "/api/v4/capture/status" => response(
            &mut connection,
            "200 OK",
            "Content-Type: application/json\r\n",
            b"{\"status\":\"ok\"}",
        )?,
        "/metrics" => {
            let body = state.metrics_json();
            response(
                &mut connection,
                "200 OK",
                "Content-Type: application/json\r\n",
                body.as_bytes(),
            )?;
        }
        "/control" => handle_control(&mut connection, &request, &state)?,
        "/preview" | "/api/v4/preview" => {
            let body = b"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: 8\r\n\r\n\xff\xd8test\xff\xd9\r\n";
            let pool = Arc::clone(&state.preview_pool);
            handle_stream(
                &mut connection,
                wake_stream,
                state,
                pool,
                body,
                "multipart/x-mixed-replace; boundary=frame",
            )?;
        }
        "/events" | "/api/v4/capture/events" | "/api/v4/network/events" => {
            let cursor = request
                .headers
                .get("last-event-id")
                .and_then(|value| value.parse().ok());
            let body = sse_payload(cursor);
            let pool = Arc::clone(&state.sse_pool);
            handle_stream(
                &mut connection,
                wake_stream,
                state,
                pool,
                &body,
                "text/event-stream",
            )?;
        }
        "/slow" => handle_slow_stream(&mut connection, wake_stream, state)?,
        "/shutdown" => {
            response(&mut connection, "200 OK", "", b"shutting down")?;
            state.initiate_shutdown();
        }
        _ => response(&mut connection, "404 Not Found", "", b"not found")?,
    }
    Ok(())
}

fn load_tls(certificate: &Path, private_key: &Path) -> Result<Arc<ServerConfig>, AnyError> {
    let mut certificate_reader = BufReader::new(File::open(certificate)?);
    let certificates =
        rustls_pemfile::certs(&mut certificate_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = BufReader::new(File::open(private_key)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or("private key PEM is empty")?;
    Ok(Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, key)?,
    ))
}

fn start_control_worker(
    receiver: Receiver<ControlRequest>,
    python: PathBuf,
    fake: PathBuf,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let result = (|| -> Result<(), AnyError> {
            let mut child = Command::new(python)
                .arg(fake)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?;
            let mut input = child.stdin.take().ok_or("missing fake stdin")?;
            let output = child.stdout.take().ok_or("missing fake stdout")?;
            let mut output = BufReader::new(output);
            for request in receiver {
                let response = (|| -> Result<String, String> {
                    writeln!(input, "{{\"delay_ms\":{}}}", request.delay_ms)
                        .map_err(|error| error.to_string())?;
                    input.flush().map_err(|error| error.to_string())?;
                    let mut line = String::new();
                    output
                        .read_line(&mut line)
                        .map_err(|error| error.to_string())?;
                    if line.is_empty() {
                        return Err("python fake closed stdout".to_owned());
                    }
                    Ok(line.trim_end().to_owned())
                })();
                let _ = request.reply.send(response);
            }
            let _ = child.kill();
            let _ = child.wait();
            Ok(())
        })();
        if let Err(error) = result {
            eprintln!("control worker failed: {error}");
        }
    })
}

fn reap_finished(handlers: &mut Vec<JoinHandle<()>>) {
    let mut index = 0;
    while index < handlers.len() {
        if handlers[index].is_finished() {
            let handler = handlers.swap_remove(index);
            let _ = handler.join();
        } else {
            index += 1;
        }
    }
}

fn main() -> Result<(), AnyError> {
    let arguments = Arguments::parse()?;
    let tls = match (&arguments.certificate, &arguments.private_key) {
        (Some(certificate), Some(private_key)) => Some(load_tls(certificate, private_key)?),
        (None, None) => None,
        _ => unreachable!("argument validation requires the TLS pair"),
    };
    let (control_sender, control_receiver) = mpsc::sync_channel(1);
    let _control_worker = start_control_worker(
        control_receiver,
        arguments.python.clone(),
        arguments.python_fake.clone(),
    );
    let state = RuntimeState::new(arguments.capacity, control_sender);
    let listener = TcpListener::bind(arguments.listen)?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    println!("{{\"port\":{},\"tls\":{}}}", address.port(), tls.is_some());
    io::stdout().flush()?;

    let mut handlers = Vec::new();
    while !state.shutdown.load(Ordering::Acquire) {
        reap_finished(&mut handlers);
        match listener.accept() {
            Ok((stream, _)) => {
                state.accepted_connections.fetch_add(1, Ordering::Relaxed);
                let connection_state = Arc::clone(&state);
                let connection_tls = tls.clone();
                handlers.push(thread::spawn(move || {
                    let _ =
                        handle_connection(stream, connection_tls, Arc::clone(&connection_state));
                    connection_state
                        .completed_connections
                        .fetch_add(1, Ordering::Relaxed);
                }));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
    state.initiate_shutdown();
    for handler in handlers {
        let _ = handler.join();
    }
    println!("{}", state.metrics_json());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permit_is_released_once_by_scope() {
        let pool = PermitPool::new(1);
        let permit = pool.acquire(Duration::ZERO).expect("first permit");
        assert_eq!(pool.active(), 1);
        assert!(pool.acquire(Duration::ZERO).is_none());
        drop(permit);
        assert_eq!(pool.active(), 0);
        assert_eq!(pool.acquired.load(Ordering::Relaxed), 1);
        assert_eq!(pool.released.load(Ordering::Relaxed), 1);
        assert_eq!(pool.rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn replay_contains_only_events_after_cursor() {
        let payload = String::from_utf8(sse_payload(Some(1))).expect("SSE is UTF-8");
        assert!(!payload.contains("id: 1\n"));
        assert!(payload.contains("id: 2\n"));
        assert!(payload.contains("id: 3\n"));
    }
}
