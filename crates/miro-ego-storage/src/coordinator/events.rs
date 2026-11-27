//! Bounded 128-slot capture event broadcaster (`miro-ego.capture-event.v1`).

use super::types::{CAPTURE_EVENT_SCHEMA, CaptureEvent, CaptureEventType, EVENT_REPLAY_CAPACITY};
use crate::util::now_rfc3339_utc;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::time::Duration;

const SUBSCRIBER_CHANNEL_CAPACITY: usize = 128;

struct BroadcasterInner {
    next_delivery_id: u64,
    replay: VecDeque<(u64, CaptureEvent)>,
    subscribers: Vec<SyncSender<CaptureEvent>>,
}

/// Non-blocking bounded event broadcaster with 128-slot replay ring for SSE reconnection.
pub struct CaptureEventBroadcaster {
    inner: Mutex<BroadcasterInner>,
}

impl CaptureEventBroadcaster {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(BroadcasterInner {
                next_delivery_id: 1,
                replay: VecDeque::with_capacity(EVENT_REPLAY_CAPACITY),
                subscribers: Vec::new(),
            }),
        }
    }

    pub fn subscribe(&self) -> CaptureEventSubscription {
        let (tx, rx) = mpsc::sync_channel(SUBSCRIBER_CHANNEL_CAPACITY);
        if let Ok(mut guard) = self.inner.lock() {
            guard.subscribers.push(tx);
        }
        CaptureEventSubscription { rx }
    }

    pub fn emit(
        &self,
        authority_epoch: &str,
        source_revision: u64,
        event_type: CaptureEventType,
        session_id: Option<String>,
        data: Value,
    ) -> CaptureEvent {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let delivery_id = guard.next_delivery_id;
        guard.next_delivery_id = guard.next_delivery_id.saturating_add(1);
        let event = CaptureEvent {
            schema: CAPTURE_EVENT_SCHEMA.to_owned(),
            sse_delivery_id: delivery_id.to_string(),
            authority_epoch: authority_epoch.to_owned(),
            source_revision,
            event_type,
            occurred_at: now_rfc3339_utc(),
            session_id,
            data,
        };
        if guard.replay.len() >= EVENT_REPLAY_CAPACITY {
            guard.replay.pop_front();
        }
        guard.replay.push_back((delivery_id, event.clone()));
        guard
            .subscribers
            .retain(|sub| match sub.try_send(event.clone()) {
                Ok(()) | Err(mpsc::TrySendError::Full(_)) => true,
                Err(mpsc::TrySendError::Disconnected(_)) => false,
            });
        event
    }

    /// Return `(needs_snapshot, events)` for an SSE client reconnecting with `Last-Event-ID`.
    pub fn replay_since(&self, last_delivery_id: Option<u64>) -> (bool, Vec<CaptureEvent>) {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(last_id) = last_delivery_id else {
            return (true, Vec::new());
        };
        let Some(&(oldest_id, _)) = guard.replay.front() else {
            return (true, Vec::new());
        };
        let latest_id = guard.next_delivery_id.saturating_sub(1);
        if last_id > latest_id || last_id.saturating_add(1) < oldest_id {
            return (true, Vec::new());
        }
        let events = guard
            .replay
            .iter()
            .filter(|(id, _)| *id > last_id)
            .map(|(_, ev)| ev.clone())
            .collect();
        (false, events)
    }
}

impl Default for CaptureEventBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

/// Subscriber handle receiving live [`CaptureEvent`] messages without blocking capture.
pub struct CaptureEventSubscription {
    rx: Receiver<CaptureEvent>,
}

impl CaptureEventSubscription {
    pub fn try_recv(&self) -> Result<CaptureEvent, TryRecvError> {
        self.rx.try_recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<CaptureEvent, RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }
}
