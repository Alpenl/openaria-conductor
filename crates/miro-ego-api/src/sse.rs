//! Server-Sent Events (`GET /api/v1/capture/events`, `GET /api/v1/network/events`)
//! with 128-slot replay buffers, `tokio::sync::broadcast` fanout, safe untrusted
//! `Last-Event-ID` handling (never advancing internal monotonic delivery IDs), and
//! configurable heartbeat keepalives.

use crate::backend::GatewayBackend;
use crate::error::{ApiError, ApiResult};
use bytes::Bytes;
use futures_util::Stream;
use serde_json::{Map, Value};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, broadcast, watch};

pub const DEFAULT_REPLAY_CAPACITY: usize = 128;
pub const CAPTURE_EVENT_SCHEMA: &str = "miro-ego.capture-event.v1";
pub const NETWORK_EVENT_SCHEMA: &str = "miro-ego.network-event.v1";

const RESYNC_ANCHOR_LIMIT: usize = 256;
const RETIRED_AUTHORITY_EPOCH_LIMIT: usize = 64;
const HEARTBEAT_BYTES: &[u8] = b": heartbeat\n\n";
const CONNECTED_COMMENT_BYTES: &[u8] = b": connected\n\n";

/// Parsed decimal `Last-Event-ID` cursor.
///
/// Stored as `u128` after validating `^[0-9]+$` and bounding length (`<= 128` digits).
/// Values larger than `u64::MAX` are represented cleanly as `CursorValue::OutOfRange`
/// so untrusted giant numbers can NEVER overflow or advance the server's monotonic counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCursor {
    Exact(u64),
    OutOfRange,
}

/// Strictly parse an HTTP `Last-Event-ID` header value (`^[0-9]+$`, max 128 digits).
pub fn parse_last_event_id(raw: Option<&str>) -> ApiResult<Option<EventCursor>> {
    let Some(text) = raw else {
        return Ok(None);
    };
    if text.is_empty() || text.len() > 128 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ApiError::bad_request(
            "Last-Event-ID must be a decimal transport delivery ID",
        ));
    }
    let trimmed = text.trim_start_matches('0');
    if trimmed.is_empty() {
        return Ok(Some(EventCursor::Exact(0)));
    }
    match trimmed.parse::<u64>() {
        Ok(id) => Ok(Some(EventCursor::Exact(id))),
        Err(_) => Ok(Some(EventCursor::OutOfRange)),
    }
}

/// One formatted SSE frame carrying a monotonic server-assigned `delivery_id`.
#[derive(Debug, Clone)]
pub struct SseEvent {
    pub delivery_id: u64,
    pub schema: &'static str,
    pub event_type: String,
    pub source_event: Value,
}

impl SseEvent {
    pub fn encode(&self) -> ApiResult<Bytes> {
        let delivery_str = self.delivery_id.to_string();
        let mut obj = match &self.source_event {
            Value::Object(map) => map.clone(),
            _ => {
                return Err(ApiError::internal(
                    "invalid_source_event",
                    "SSE source event must be a JSON object",
                ));
            }
        };
        obj.insert("schema".to_owned(), Value::String(self.schema.to_owned()));
        obj.insert(
            "sse_delivery_id".to_owned(),
            Value::String(delivery_str.clone()),
        );
        let json_data = serde_json::to_string(&Value::Object(obj)).map_err(|err| {
            ApiError::internal(
                "invalid_source_event",
                format!("serialize SSE event: {err}"),
            )
        })?;
        let frame = format!(
            "id: {delivery_str}\nevent: {}\ndata: {json_data}\n\n",
            self.event_type
        );
        Ok(Bytes::from(frame))
    }
}

#[derive(Debug, Clone)]
struct BufferedEvent {
    delivery_id: u64,
    publish_index: u64,
    event_type: String,
    source_event: Value,
}

#[derive(Debug, Clone)]
struct ResyncAnchor {
    next_publish_index: u64,
    source_event: Value,
}

struct ReplayInner {
    capacity: usize,
    events: VecDeque<BufferedEvent>,
    resync_anchors: HashMap<u64, ResyncAnchor>,
    resync_order: VecDeque<u64>,
    next_delivery_id: u64,
    publish_count: u64,
}

/// Bounded 128-slot replay ring + `tokio::sync::broadcast` fanout for `miro-ego.capture-event.v1`.
pub struct EventReplayBuffer {
    inner: Mutex<ReplayInner>,
    tx: broadcast::Sender<u64>,
}

impl Default for EventReplayBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_REPLAY_CAPACITY)
    }
}

impl EventReplayBuffer {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        let (tx, _) = broadcast::channel(cap.saturating_mul(2).max(64));
        Self {
            inner: Mutex::new(ReplayInner {
                capacity: cap,
                events: VecDeque::with_capacity(cap),
                resync_anchors: HashMap::new(),
                resync_order: VecDeque::new(),
                next_delivery_id: 1,
                publish_count: 0,
            }),
            tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<u64> {
        self.tx.subscribe()
    }

    /// Return the next monotonic delivery ID that will be allocated (useful for diagnostics/tests).
    pub fn next_delivery_id(&self) -> ApiResult<u64> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("state_poisoned", "capture replay buffer poisoned"))?;
        Ok(guard.next_delivery_id)
    }

    /// Publish one validated daemon capture source event, assigning the next monotonic delivery ID.
    pub fn publish(&self, source_event: Value) -> ApiResult<u64> {
        let event_type = extract_event_type(&source_event)?;
        let delivery_id = {
            let mut guard = self.inner.lock().map_err(|_| {
                ApiError::internal("state_poisoned", "capture replay buffer poisoned")
            })?;
            let id = guard.next_delivery_id;
            guard.next_delivery_id = guard.next_delivery_id.saturating_add(1);
            let publish_index = guard.publish_count;
            guard.publish_count = guard.publish_count.saturating_add(1);
            if guard.events.len() == guard.capacity {
                guard.events.pop_front();
            }
            guard.events.push_back(BufferedEvent {
                delivery_id: id,
                publish_index,
                event_type,
                source_event,
            });
            id
        };
        let _ = self.tx.send(delivery_id);
        Ok(delivery_id)
    }

    /// Replay buffered events after `cursor`, or emit a fresh snapshot if `cursor` is absent,
    /// evicted, out-of-range, or non-contiguous.
    ///
    /// Untrusted `Last-Event-ID` values (including huge numbers) NEVER advance `next_delivery_id`.
    pub fn replay_or_snapshot<F>(
        &self,
        cursor: Option<EventCursor>,
        snapshot_fn: F,
    ) -> ApiResult<Vec<SseEvent>>
    where
        F: Fn() -> ApiResult<Value>,
    {
        let after_cursor = match cursor {
            Some(EventCursor::Exact(cursor_id)) => {
                let guard = self.inner.lock().map_err(|_| {
                    ApiError::internal("state_poisoned", "capture replay buffer poisoned")
                })?;
                if let Some(replayed) = replay_after_locked(&guard, cursor_id) {
                    return Ok(replayed);
                }
                Some(cursor_id)
            }
            _ => None,
        };
        let snapshot_event = self.issue_resync_snapshot_with_cursor(after_cursor, snapshot_fn)?;
        Ok(vec![snapshot_event])
    }

    /// Async variant of [`Self::replay_or_snapshot`] that evaluates `snapshot_fn` inside the
    /// optimistic `publish_count` retry loop so concurrent publishes during `.await` are never dropped.
    pub async fn replay_or_snapshot_async<F, Fut>(
        &self,
        cursor: Option<EventCursor>,
        snapshot_fn: F,
    ) -> ApiResult<Vec<SseEvent>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ApiResult<Value>>,
    {
        let after_cursor = match cursor {
            Some(EventCursor::Exact(cursor_id)) => {
                let guard = self.inner.lock().map_err(|_| {
                    ApiError::internal("state_poisoned", "capture replay buffer poisoned")
                })?;
                if let Some(replayed) = replay_after_locked(&guard, cursor_id) {
                    return Ok(replayed);
                }
                Some(cursor_id)
            }
            _ => None,
        };
        let snapshot_event = self
            .issue_resync_snapshot_with_cursor_async(after_cursor, snapshot_fn)
            .await?;
        Ok(vec![snapshot_event])
    }

    /// Collect events strictly after `cursor_id`, or `None` when a fresh resync snapshot is needed.
    pub fn try_events_after(&self, cursor_id: u64) -> ApiResult<Option<Vec<SseEvent>>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("state_poisoned", "capture replay buffer poisoned"))?;
        Ok(replay_after_locked(&guard, cursor_id))
    }

    pub fn issue_resync_snapshot<F>(&self, snapshot_fn: F) -> ApiResult<SseEvent>
    where
        F: Fn() -> ApiResult<Value>,
    {
        self.issue_resync_snapshot_with_cursor(None, snapshot_fn)
    }

    pub fn issue_resync_snapshot_with_cursor<F>(
        &self,
        after_cursor: Option<u64>,
        snapshot_fn: F,
    ) -> ApiResult<SseEvent>
    where
        F: Fn() -> ApiResult<Value>,
    {
        let mut last_snapshot = None;
        let mut publish_before = 0;
        for _ in 0..8 {
            publish_before = {
                let guard = self.inner.lock().map_err(|_| {
                    ApiError::internal("state_poisoned", "capture replay buffer poisoned")
                })?;
                guard.publish_count
            };
            let snap = snapshot_fn()?;
            let event_type = extract_event_type(&snap)?;
            if event_type != "snapshot" {
                return Err(ApiError::internal(
                    "invalid_source_event",
                    "capture snapshot callback must return type=snapshot",
                ));
            }
            let mut guard = self.inner.lock().map_err(|_| {
                ApiError::internal("state_poisoned", "capture replay buffer poisoned")
            })?;
            if guard.publish_count == publish_before {
                return Ok(allocate_resync_anchor_locked(
                    &mut guard,
                    snap,
                    publish_before,
                    after_cursor,
                    CAPTURE_EVENT_SCHEMA,
                ));
            }
            last_snapshot = Some(snap);
        }
        let snap = match last_snapshot {
            Some(s) => s,
            None => snapshot_fn()?,
        };
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("state_poisoned", "capture replay buffer poisoned"))?;
        Ok(allocate_resync_anchor_locked(
            &mut guard,
            snap,
            publish_before,
            after_cursor,
            CAPTURE_EVENT_SCHEMA,
        ))
    }

    /// Async variant of [`Self::issue_resync_snapshot`] that fetches a fresh authoritative
    /// snapshot without holding the internal mutex across `.await`.
    pub async fn issue_resync_snapshot_async<F, Fut>(&self, snapshot_fn: F) -> ApiResult<SseEvent>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ApiResult<Value>>,
    {
        self.issue_resync_snapshot_with_cursor_async(None, snapshot_fn)
            .await
    }

    pub async fn issue_resync_snapshot_with_cursor_async<F, Fut>(
        &self,
        after_cursor: Option<u64>,
        mut snapshot_fn: F,
    ) -> ApiResult<SseEvent>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ApiResult<Value>>,
    {
        let mut last_snapshot = None;
        let mut publish_before = 0;
        for _ in 0..8 {
            publish_before = {
                let guard = self.inner.lock().map_err(|_| {
                    ApiError::internal("state_poisoned", "capture replay buffer poisoned")
                })?;
                guard.publish_count
            };
            let snap = snapshot_fn().await?;
            let event_type = extract_event_type(&snap)?;
            if event_type != "snapshot" {
                return Err(ApiError::internal(
                    "invalid_source_event",
                    "capture snapshot callback must return type=snapshot",
                ));
            }
            let mut guard = self.inner.lock().map_err(|_| {
                ApiError::internal("state_poisoned", "capture replay buffer poisoned")
            })?;
            if guard.publish_count == publish_before {
                return Ok(allocate_resync_anchor_locked(
                    &mut guard,
                    snap,
                    publish_before,
                    after_cursor,
                    CAPTURE_EVENT_SCHEMA,
                ));
            }
            last_snapshot = Some(snap);
        }
        let snap = match last_snapshot {
            Some(s) => s,
            None => snapshot_fn().await?,
        };
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("state_poisoned", "capture replay buffer poisoned"))?;
        Ok(allocate_resync_anchor_locked(
            &mut guard,
            snap,
            publish_before,
            after_cursor,
            CAPTURE_EVENT_SCHEMA,
        ))
    }
}

fn allocate_resync_anchor_locked(
    guard: &mut ReplayInner,
    source_event: Value,
    next_publish_index: u64,
    after_cursor: Option<u64>,
    schema: &'static str,
) -> SseEvent {
    // Never allow an untrusted Last-Event-ID to jump `next_delivery_id` arbitrarily;
    // only bump by 1 on an exact collision with the next sequential ID.
    if let Some(cursor) = after_cursor
        && cursor == guard.next_delivery_id
    {
        guard.next_delivery_id = guard.next_delivery_id.saturating_add(1);
    }
    let delivery_id = guard.next_delivery_id;
    guard.next_delivery_id = guard.next_delivery_id.saturating_add(1);
    guard.resync_anchors.insert(
        delivery_id,
        ResyncAnchor {
            next_publish_index,
            source_event: source_event.clone(),
        },
    );
    guard.resync_order.push_back(delivery_id);
    while guard.resync_order.len() > RESYNC_ANCHOR_LIMIT {
        if let Some(evicted) = guard.resync_order.pop_front() {
            guard.resync_anchors.remove(&evicted);
        }
    }
    SseEvent {
        delivery_id,
        schema,
        event_type: "snapshot".to_owned(),
        source_event,
    }
}

fn replay_after_locked(guard: &ReplayInner, cursor: u64) -> Option<Vec<SseEvent>> {
    if let Some(anchor) = guard.resync_anchors.get(&cursor) {
        if guard.events.is_empty() {
            return if anchor.next_publish_index == guard.publish_count {
                Some(Vec::new())
            } else {
                None
            };
        }
        let oldest_index = guard.events.front()?.publish_index;
        if anchor.next_publish_index < oldest_index
            || anchor.next_publish_index > guard.publish_count
        {
            return None;
        }
        let start = usize::try_from(anchor.next_publish_index - oldest_index).ok()?;
        let slice: Vec<&BufferedEvent> = guard.events.iter().skip(start).collect();
        if slice.is_empty() {
            return Some(Vec::new());
        }
        if !source_pair_is_contiguous(&anchor.source_event, &slice[0].source_event)
            || !buffered_slice_is_contiguous(&slice)
        {
            return None;
        }
        return Some(
            slice
                .into_iter()
                .map(|ev| SseEvent {
                    delivery_id: ev.delivery_id,
                    schema: CAPTURE_EVENT_SCHEMA,
                    event_type: ev.event_type.clone(),
                    source_event: ev.source_event.clone(),
                })
                .collect(),
        );
    }

    let first = guard.events.front()?;
    let last = guard.events.back()?;
    if cursor < first.delivery_id || cursor > last.delivery_id {
        return None;
    }
    let idx = guard
        .events
        .iter()
        .position(|item| item.delivery_id == cursor)?;
    let tail: Vec<&BufferedEvent> = guard.events.iter().skip(idx).collect();
    if tail.len() <= 1 {
        return Some(Vec::new());
    }
    if !buffered_slice_is_contiguous(&tail) {
        return None;
    }
    Some(
        tail.into_iter()
            .skip(1)
            .map(|ev| SseEvent {
                delivery_id: ev.delivery_id,
                schema: CAPTURE_EVENT_SCHEMA,
                event_type: ev.event_type.clone(),
                source_event: ev.source_event.clone(),
            })
            .collect(),
    )
}

fn source_pair_is_contiguous(previous: &Value, current: &Value) -> bool {
    if previous.get("authority_epoch") != current.get("authority_epoch") {
        return false;
    }
    let Some(prev_rev) = previous.get("source_revision").and_then(Value::as_u64) else {
        return false;
    };
    let Some(curr_rev) = current.get("source_revision").and_then(Value::as_u64) else {
        return false;
    };
    if curr_rev == prev_rev {
        return true;
    }
    prev_rev.checked_add(1) == Some(curr_rev)
}

fn buffered_slice_is_contiguous(events: &[&BufferedEvent]) -> bool {
    events
        .windows(2)
        .all(|pair| source_pair_is_contiguous(&pair[0].source_event, &pair[1].source_event))
}

fn extract_event_type(event: &Value) -> ApiResult<String> {
    let Some(event_type) = event.get("type").and_then(Value::as_str) else {
        return Err(ApiError::internal(
            "invalid_source_event",
            "SSE source event missing string type field",
        ));
    };
    if event_type.is_empty()
        || event_type
            .bytes()
            .any(|b| b == b'\r' || b == b'\n' || b < 0x20)
    {
        return Err(ApiError::internal(
            "invalid_source_event",
            "SSE source event type contains illegal characters",
        ));
    }
    Ok(event_type.to_owned())
}

// ---------------------------------------------------------------------------
// Network SSE replay buffer (`miro-ego.network-event.v1`)
// ---------------------------------------------------------------------------

struct NetworkReplayInner {
    replay: ReplayInner,
    last_status: Option<Value>,
    retired_epochs: VecDeque<String>,
}

/// Bounded 128-slot replay ring + `tokio::sync::broadcast` fanout for `miro-ego.network-event.v1`.
pub struct NetworkEventReplayBuffer {
    inner: Mutex<NetworkReplayInner>,
    tx: broadcast::Sender<u64>,
}

impl Default for NetworkEventReplayBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_REPLAY_CAPACITY)
    }
}

impl NetworkEventReplayBuffer {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        let (tx, _) = broadcast::channel(cap.saturating_mul(2).max(64));
        Self {
            inner: Mutex::new(NetworkReplayInner {
                replay: ReplayInner {
                    capacity: cap,
                    events: VecDeque::with_capacity(cap),
                    resync_anchors: HashMap::new(),
                    resync_order: VecDeque::new(),
                    next_delivery_id: 1,
                    publish_count: 0,
                },
                last_status: None,
                retired_epochs: VecDeque::new(),
            }),
            tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<u64> {
        self.tx.subscribe()
    }

    pub fn next_delivery_id(&self) -> ApiResult<u64> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("state_poisoned", "network replay buffer poisoned"))?;
        Ok(guard.replay.next_delivery_id)
    }

    /// Observe a new `miro-ego.network-status.v1` snapshot and publish a `transaction` or
    /// `snapshot` event if state or authority progressed.
    pub fn observe(&self, status: Value) -> ApiResult<Option<u64>> {
        let published = {
            let mut guard = self.inner.lock().map_err(|_| {
                ApiError::internal("state_poisoned", "network replay buffer poisoned")
            })?;
            observe_network_locked(&mut guard, status)?
        };
        if let Some(id) = published {
            let _ = self.tx.send(id);
        }
        Ok(published)
    }

    /// Replay buffered network events after `cursor`, or emit a resync snapshot.
    ///
    /// Untrusted `Last-Event-ID` values (including huge numbers) NEVER jump `next_delivery_id`.
    /// Any state transition newly observed in `status` is also broadcast to live SSE subscribers.
    pub fn replay_or_snapshot(
        &self,
        cursor: Option<EventCursor>,
        status: Value,
    ) -> ApiResult<Vec<SseEvent>> {
        let (events, published) = {
            let mut guard = self.inner.lock().map_err(|_| {
                ApiError::internal("state_poisoned", "network replay buffer poisoned")
            })?;
            match cursor {
                None | Some(EventCursor::OutOfRange) => {
                    let (snap, pub_id) = issue_network_resync_locked(&mut guard, status, None)?;
                    (vec![snap], pub_id)
                }
                Some(EventCursor::Exact(cursor_id)) => {
                    if network_replay_after_locked(&guard.replay, cursor_id).is_none() {
                        let (snap, pub_id) =
                            issue_network_resync_locked(&mut guard, status, Some(cursor_id))?;
                        (vec![snap], pub_id)
                    } else {
                        let published_obs = observe_network_locked(&mut guard, status.clone())?;
                        if let Some(replayed) =
                            network_replay_after_locked(&guard.replay, cursor_id)
                        {
                            (replayed, published_obs)
                        } else {
                            let (snap, pub_resync) =
                                issue_network_resync_locked(&mut guard, status, Some(cursor_id))?;
                            (vec![snap], pub_resync.or(published_obs))
                        }
                    }
                }
            }
        };
        if let Some(id) = published {
            let _ = self.tx.send(id);
        }
        Ok(events)
    }

    pub fn try_events_after(&self, cursor_id: u64) -> ApiResult<Option<Vec<SseEvent>>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("state_poisoned", "network replay buffer poisoned"))?;
        Ok(network_replay_after_locked(&guard.replay, cursor_id))
    }

    pub fn issue_resync_snapshot(
        &self,
        status: Value,
        after_cursor: Option<u64>,
    ) -> ApiResult<SseEvent> {
        let (snap, published) = {
            let mut guard = self.inner.lock().map_err(|_| {
                ApiError::internal("state_poisoned", "network replay buffer poisoned")
            })?;
            issue_network_resync_locked(&mut guard, status, after_cursor)?
        };
        if let Some(id) = published {
            let _ = self.tx.send(id);
        }
        Ok(snap)
    }
}

fn network_pair_is_contiguous(previous: &Value, current: &Value) -> bool {
    if current.get("type").and_then(Value::as_str) == Some("snapshot") {
        return true;
    }
    source_pair_is_contiguous(previous, current)
}

fn network_slice_is_contiguous(events: &[&BufferedEvent]) -> bool {
    events
        .windows(2)
        .all(|pair| network_pair_is_contiguous(&pair[0].source_event, &pair[1].source_event))
}

fn network_replay_after_locked(replay: &ReplayInner, cursor: u64) -> Option<Vec<SseEvent>> {
    if let Some(anchor) = replay.resync_anchors.get(&cursor) {
        if replay.events.is_empty() {
            return if anchor.next_publish_index == replay.publish_count {
                Some(Vec::new())
            } else {
                None
            };
        }
        let oldest_index = replay.events.front()?.publish_index;
        if anchor.next_publish_index < oldest_index
            || anchor.next_publish_index > replay.publish_count
        {
            return None;
        }
        let start = usize::try_from(anchor.next_publish_index - oldest_index).ok()?;
        let slice: Vec<&BufferedEvent> = replay.events.iter().skip(start).collect();
        if slice.is_empty() {
            return Some(Vec::new());
        }
        if !network_pair_is_contiguous(&anchor.source_event, &slice[0].source_event)
            || !network_slice_is_contiguous(&slice)
        {
            return None;
        }
        return Some(
            slice
                .into_iter()
                .map(|ev| SseEvent {
                    delivery_id: ev.delivery_id,
                    schema: NETWORK_EVENT_SCHEMA,
                    event_type: ev.event_type.clone(),
                    source_event: ev.source_event.clone(),
                })
                .collect(),
        );
    }
    let first = replay.events.front()?;
    let last = replay.events.back()?;
    if cursor < first.delivery_id || cursor > last.delivery_id {
        return None;
    }
    let idx = replay
        .events
        .iter()
        .position(|item| item.delivery_id == cursor)?;
    let tail: Vec<&BufferedEvent> = replay.events.iter().skip(idx).collect();
    if tail.len() <= 1 {
        return Some(Vec::new());
    }
    if !network_slice_is_contiguous(&tail) {
        return None;
    }
    Some(
        tail.into_iter()
            .skip(1)
            .map(|ev| SseEvent {
                delivery_id: ev.delivery_id,
                schema: NETWORK_EVENT_SCHEMA,
                event_type: ev.event_type.clone(),
                source_event: ev.source_event.clone(),
            })
            .collect(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkRelation {
    New,
    NewAuthority,
    Projection,
    Same,
    Stale,
}

fn network_status_relation(
    guard: &NetworkReplayInner,
    status: &Value,
) -> ApiResult<NetworkRelation> {
    let Some(last) = &guard.last_status else {
        return Ok(NetworkRelation::New);
    };
    let authority = status
        .get("authority_epoch")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ApiError::internal(
                "invalid_source_state",
                "network status missing authority_epoch",
            )
        })?;
    let rev = status
        .get("source_revision")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ApiError::internal(
                "invalid_source_state",
                "network status missing source_revision",
            )
        })?;
    let last_authority = last
        .get("authority_epoch")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let last_rev = last
        .get("source_revision")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if authority == last_authority {
        if rev < last_rev {
            return Ok(NetworkRelation::Stale);
        }
        if rev > last_rev {
            return Ok(NetworkRelation::New);
        }
        const PROJECTION_FIELDS: [&str; 7] = [
            "saved",
            "verified",
            "desired",
            "observed",
            "transaction",
            "mutation_capability",
            "concurrency_capability",
        ];
        if PROJECTION_FIELDS
            .iter()
            .any(|field| status.get(*field) != last.get(*field))
        {
            return Ok(NetworkRelation::Projection);
        }
        return Ok(NetworkRelation::Same);
    }
    if guard.retired_epochs.iter().any(|e| e == authority) {
        return Ok(NetworkRelation::Stale);
    }
    Ok(NetworkRelation::NewAuthority)
}

fn observe_network_locked(guard: &mut NetworkReplayInner, status: Value) -> ApiResult<Option<u64>> {
    let relation = network_status_relation(guard, &status)?;
    if matches!(relation, NetworkRelation::Same | NetworkRelation::Stale) {
        return Ok(None);
    }
    let authority = status
        .get("authority_epoch")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ApiError::internal(
                "invalid_source_state",
                "network status missing authority_epoch",
            )
        })?
        .to_owned();
    let source_revision = status
        .get("source_revision")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ApiError::internal(
                "invalid_source_state",
                "network status missing source_revision",
            )
        })?;
    let observed_at = status
        .get("observed_at")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ApiError::internal("invalid_source_state", "network status missing observed_at")
        })?
        .to_owned();

    let skipped_revision = matches!(relation, NetworkRelation::New)
        && guard
            .last_status
            .as_ref()
            .and_then(|last| last.get("source_revision"))
            .and_then(Value::as_u64)
            .is_some_and(|last_rev| last_rev.checked_add(1) != Some(source_revision));
    let force_snapshot = matches!(
        relation,
        NetworkRelation::NewAuthority | NetworkRelation::Projection
    ) || skipped_revision;

    let matching_tx = if !force_snapshot {
        status
            .get("transaction")
            .and_then(Value::as_object)
            .and_then(|win| {
                win.get("current")
                    .filter(|v| !v.is_null())
                    .or_else(|| win.get("latest").filter(|v| !v.is_null()))
            })
            .filter(|tx| {
                tx.get("authority_epoch").and_then(Value::as_str) == Some(authority.as_str())
                    && tx.get("source_revision").and_then(Value::as_u64) == Some(source_revision)
            })
            .cloned()
    } else {
        None
    };

    let (event_type, source_event) = if let Some(tx) = matching_tx {
        let updated_at = tx
            .get("updated_at")
            .and_then(Value::as_str)
            .unwrap_or(&observed_at)
            .to_owned();
        let tx_id = tx.get("transaction_id").cloned().unwrap_or(Value::Null);
        let mut map = Map::new();
        map.insert(
            "authority_epoch".to_owned(),
            Value::String(authority.clone()),
        );
        map.insert("source_revision".to_owned(), Value::from(source_revision));
        map.insert("occurred_at".to_owned(), Value::String(updated_at));
        map.insert("type".to_owned(), Value::String("transaction".to_owned()));
        map.insert("transaction_id".to_owned(), tx_id);
        map.insert("data".to_owned(), tx);
        ("transaction".to_owned(), Value::Object(map))
    } else {
        let mut map = Map::new();
        map.insert(
            "authority_epoch".to_owned(),
            Value::String(authority.clone()),
        );
        map.insert("source_revision".to_owned(), Value::from(source_revision));
        map.insert("occurred_at".to_owned(), Value::String(observed_at));
        map.insert("type".to_owned(), Value::String("snapshot".to_owned()));
        map.insert("transaction_id".to_owned(), Value::Null);
        map.insert("data".to_owned(), status.clone());
        ("snapshot".to_owned(), Value::Object(map))
    };

    let delivery_id = guard.replay.next_delivery_id;
    guard.replay.next_delivery_id = guard.replay.next_delivery_id.saturating_add(1);
    let publish_index = guard.replay.publish_count;
    guard.replay.publish_count = guard.replay.publish_count.saturating_add(1);
    if guard.replay.events.len() == guard.replay.capacity {
        guard.replay.events.pop_front();
    }
    guard.replay.events.push_back(BufferedEvent {
        delivery_id,
        publish_index,
        event_type,
        source_event,
    });

    if let Some(prev) = &guard.last_status
        && let Some(prev_auth) = prev.get("authority_epoch").and_then(Value::as_str)
        && prev_auth != authority
    {
        let prev_owned = prev_auth.to_owned();
        if !guard.retired_epochs.iter().any(|e| e == &prev_owned) {
            guard.retired_epochs.push_back(prev_owned);
            while guard.retired_epochs.len() > RETIRED_AUTHORITY_EPOCH_LIMIT {
                guard.retired_epochs.pop_front();
            }
        }
    }
    guard.last_status = Some(status);
    Ok(Some(delivery_id))
}

fn issue_network_resync_locked(
    guard: &mut NetworkReplayInner,
    status: Value,
    after_cursor: Option<u64>,
) -> ApiResult<(SseEvent, Option<u64>)> {
    let published = if guard.last_status.is_some() {
        observe_network_locked(guard, status.clone())?
    } else {
        None
    };
    let selected = match &guard.last_status {
        Some(existing) => existing.clone(),
        None => {
            guard.last_status = Some(status.clone());
            status
        }
    };
    let authority = selected.get("authority_epoch").cloned().ok_or_else(|| {
        ApiError::internal(
            "invalid_source_state",
            "network status missing authority_epoch",
        )
    })?;
    let source_revision = selected.get("source_revision").cloned().ok_or_else(|| {
        ApiError::internal(
            "invalid_source_state",
            "network status missing source_revision",
        )
    })?;
    let observed_at = selected.get("observed_at").cloned().ok_or_else(|| {
        ApiError::internal("invalid_source_state", "network status missing observed_at")
    })?;

    let mut map = Map::new();
    map.insert("authority_epoch".to_owned(), authority);
    map.insert("source_revision".to_owned(), source_revision);
    map.insert("occurred_at".to_owned(), observed_at);
    map.insert("type".to_owned(), Value::String("snapshot".to_owned()));
    map.insert("transaction_id".to_owned(), Value::Null);
    map.insert("data".to_owned(), selected);

    let publish_index = guard.replay.publish_count;
    let event = allocate_resync_anchor_locked(
        &mut guard.replay,
        Value::Object(map),
        publish_index,
        after_cursor,
        NETWORK_EVENT_SCHEMA,
    );
    Ok((event, published))
}

// ---------------------------------------------------------------------------
// Async SSE response body stream (holds SSE concurrency semaphore permit)
// ---------------------------------------------------------------------------

/// Await a `true` shutdown signal on `rx`. If all senders drop without signaling `true`,
/// park forever so standalone unit-test streams without an active server do not abort early.
pub(crate) async fn wait_for_shutdown(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow_and_update() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow_and_update() {
            return;
        }
    }
    if !*rx.borrow() {
        std::future::pending::<()>().await;
    }
}

/// Build an async `axum::body::Body` for capture SSE streaming.
///
/// `sub` MUST be subscribed BEFORE calling `replay_or_snapshot` so events published between
/// replay and live streaming never miss a broadcast wakeup. Lagged resyncs always fetch a
/// fresh authoritative snapshot from `backend`.
#[allow(clippy::too_many_arguments)]
pub fn capture_sse_body(
    buffer: Arc<EventReplayBuffer>,
    mut sub: broadcast::Receiver<u64>,
    backend: Arc<dyn GatewayBackend>,
    initial_events: Vec<SseEvent>,
    initial_cursor: u64,
    heartbeat_interval: Duration,
    permit: OwnedSemaphorePermit,
    mut shutdown_rx: watch::Receiver<bool>,
) -> axum::body::Body {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
    tokio::spawn(async move {
        let _permit = permit;
        let mut cursor = initial_cursor;

        if initial_events.is_empty() {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown_rx) => return,
                res = tx.send(Ok(Bytes::from_static(CONNECTED_COMMENT_BYTES))) => {
                    if res.is_err() {
                        return;
                    }
                }
            }
        } else {
            for event in initial_events {
                match event.encode() {
                    Ok(chunk) => {
                        cursor = event.delivery_id;
                        tokio::select! {
                            biased;
                            _ = wait_for_shutdown(&mut shutdown_rx) => return,
                            res = tx.send(Ok(chunk)) => {
                                if res.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Err(_) => return,
                }
            }
        }

        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await; // consume immediate initial tick

        loop {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown_rx) => return,
                _ = tx.closed() => return,
                recv = sub.recv() => {
                    match recv {
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                            let batch = match buffer.try_events_after(cursor) {
                                Ok(Some(events)) => events,
                                Ok(None) => {
                                    let backend_ref = Arc::clone(&backend);
                                    let resync_fut = buffer
                                        .issue_resync_snapshot_with_cursor_async(
                                            Some(cursor),
                                            || {
                                                let b = Arc::clone(&backend_ref);
                                                async move { b.capture_snapshot_event().await }
                                            },
                                        );
                                    tokio::select! {
                                        biased;
                                        _ = wait_for_shutdown(&mut shutdown_rx) => return,
                                        res = resync_fut => match res {
                                            Ok(snap) => vec![snap],
                                            Err(_) => return,
                                        },
                                    }
                                }
                                Err(_) => return,
                            };
                            for event in batch {
                                match event.encode() {
                                    Ok(chunk) => {
                                        cursor = event.delivery_id;
                                        tokio::select! {
                                            biased;
                                            _ = wait_for_shutdown(&mut shutdown_rx) => return,
                                            res = tx.send(Ok(chunk)) => {
                                                if res.is_err() {
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                    Err(_) => return,
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
                _ = heartbeat.tick() => {
                    tokio::select! {
                        biased;
                        _ = wait_for_shutdown(&mut shutdown_rx) => return,
                        res = tx.send(Ok(Bytes::from_static(HEARTBEAT_BYTES))) => {
                            if res.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    });
    axum::body::Body::from_stream(ReceiverByteStream(rx))
}

/// Build an async `axum::body::Body` for network SSE streaming.
///
/// `sub` MUST be subscribed BEFORE calling `replay_or_snapshot`. Lagged resyncs always fetch
/// fresh authoritative network status from `backend`.
#[allow(clippy::too_many_arguments)]
pub fn network_sse_body(
    buffer: Arc<NetworkEventReplayBuffer>,
    mut sub: broadcast::Receiver<u64>,
    backend: Arc<dyn GatewayBackend>,
    initial_events: Vec<SseEvent>,
    initial_cursor: u64,
    heartbeat_interval: Duration,
    permit: OwnedSemaphorePermit,
    mut shutdown_rx: watch::Receiver<bool>,
) -> axum::body::Body {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
    tokio::spawn(async move {
        let _permit = permit;
        let mut cursor = initial_cursor;

        if initial_events.is_empty() {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown_rx) => return,
                res = tx.send(Ok(Bytes::from_static(CONNECTED_COMMENT_BYTES))) => {
                    if res.is_err() {
                        return;
                    }
                }
            }
        } else {
            for event in initial_events {
                match event.encode() {
                    Ok(chunk) => {
                        cursor = event.delivery_id;
                        tokio::select! {
                            biased;
                            _ = wait_for_shutdown(&mut shutdown_rx) => return,
                            res = tx.send(Ok(chunk)) => {
                                if res.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Err(_) => return,
                }
            }
        }

        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown_rx) => return,
                _ = tx.closed() => return,
                recv = sub.recv() => {
                    match recv {
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                            let batch = match buffer.try_events_after(cursor) {
                                Ok(Some(events)) => events,
                                Ok(None) => {
                                    let status_fut = backend.network_status();
                                    let status_res = tokio::select! {
                                        biased;
                                        _ = wait_for_shutdown(&mut shutdown_rx) => return,
                                        res = status_fut => res,
                                    };
                                    match status_res.and_then(|s| buffer.issue_resync_snapshot(s, Some(cursor))) {
                                        Ok(snap) => vec![snap],
                                        Err(_) => return,
                                    }
                                }
                                Err(_) => return,
                            };
                            for event in batch {
                                match event.encode() {
                                    Ok(chunk) => {
                                        cursor = event.delivery_id;
                                        tokio::select! {
                                            biased;
                                            _ = wait_for_shutdown(&mut shutdown_rx) => return,
                                            res = tx.send(Ok(chunk)) => {
                                                if res.is_err() {
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                    Err(_) => return,
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
                _ = heartbeat.tick() => {
                    tokio::select! {
                        biased;
                        _ = wait_for_shutdown(&mut shutdown_rx) => return,
                        res = tx.send(Ok(Bytes::from_static(HEARTBEAT_BYTES))) => {
                            if res.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    });
    axum::body::Body::from_stream(ReceiverByteStream(rx))
}

struct ReceiverByteStream(tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>);

impl Stream for ReceiverByteStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_snapshot(epoch: &str, rev: u64) -> Value {
        json!({
            "authority_epoch": epoch,
            "source_revision": rev,
            "type": "snapshot",
            "occurred_at": "2026-11-26T10:00:00Z",
            "session_id": null,
            "data": { "schema": "miro-ego.capture-snapshot-event.v1", "device_state": "idle" }
        })
    }

    fn sample_state(epoch: &str, rev: u64, session_id: &str) -> Value {
        json!({
            "authority_epoch": epoch,
            "source_revision": rev,
            "type": "state",
            "occurred_at": "2026-11-26T10:00:01Z",
            "session_id": session_id,
            "data": { "schema": "miro-ego.capture-state-event.v1", "state": "recording" }
        })
    }

    #[test]
    fn giant_untrusted_last_event_id_never_advances_monotonic_counter() {
        let buf = EventReplayBuffer::new(128);
        let epoch = miro_ego_core::ids::new_uuid_v4();
        let before = buf.next_delivery_id().unwrap();
        assert_eq!(before, 1);

        let giant = parse_last_event_id(Some("99999999999999999999999999999999999999")).unwrap();
        assert_eq!(giant, Some(EventCursor::OutOfRange));
        let events = buf
            .replay_or_snapshot(giant, || Ok(sample_snapshot(&epoch, 1)))
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].delivery_id, 1);
        assert_eq!(buf.next_delivery_id().unwrap(), 2);

        let large_u64 = parse_last_event_id(Some("18446744073709551614")).unwrap();
        let events2 = buf
            .replay_or_snapshot(large_u64, || Ok(sample_snapshot(&epoch, 1)))
            .unwrap();
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].delivery_id, 2);
        assert_eq!(buf.next_delivery_id().unwrap(), 3);
    }

    #[test]
    fn replays_contiguous_events_and_resyncs_on_eviction() {
        let buf = EventReplayBuffer::new(4);
        let epoch = miro_ego_core::ids::new_uuid_v4();
        let sid = miro_ego_core::ids::new_uuid_v7();
        let id1 = buf.publish(sample_state(&epoch, 1, &sid)).unwrap();
        let id2 = buf.publish(sample_state(&epoch, 2, &sid)).unwrap();
        let id3 = buf.publish(sample_state(&epoch, 3, &sid)).unwrap();

        let replayed = buf
            .replay_or_snapshot(Some(EventCursor::Exact(id1)), || {
                Ok(sample_snapshot(&epoch, 3))
            })
            .unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].delivery_id, id2);
        assert_eq!(replayed[1].delivery_id, id3);

        // Push beyond capacity to evict id1
        for rev in 4..=8 {
            buf.publish(sample_state(&epoch, rev, &sid)).unwrap();
        }
        let resynced = buf
            .replay_or_snapshot(Some(EventCursor::Exact(id1)), || {
                Ok(sample_snapshot(&epoch, 8))
            })
            .unwrap();
        assert_eq!(resynced.len(), 1);
        assert_eq!(resynced[0].event_type, "snapshot");
    }
}
