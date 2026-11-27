//! Gateway-level bounded `Idempotency-Key` in-flight request coalescer and 24-hour replay cache.
//!
//! Deduplicates concurrent duplicate retries (`tokio::sync::watch`) so slow hardware mutations
//! (camera start/stop, V4L2 focus, nmcli apply/retry/forget, firmware switch) execute at most once
//! per `(principal_id, scope, idempotency_key)`, while replaying completed `2xx` outcomes with
//! `Idempotency-Replayed: true` and rejecting mismatched payloads with `409 idempotency_conflict`.

use crate::backend::CommandOutcome;
use crate::error::{ApiError, ApiResult};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub const DEFAULT_IDEMPOTENCY_CAPACITY: usize = 512;
pub const DEFAULT_IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Shared idempotency scope across `/network/apply`, `/network/retry`, and `/network/forget`.
pub const NETWORK_MUTATION_SCOPE: &str = "network-mutation";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CoalesceKey {
    principal_id: String,
    scope: String,
    idempotency_key: String,
}

#[derive(Debug, Clone)]
struct CompletedRecord {
    operation_id: String,
    canonical_body: Vec<u8>,
    outcome: CommandOutcome,
    expires_at: Instant,
}

struct InFlightRecord {
    operation_id: String,
    canonical_body: Vec<u8>,
    rx: watch::Receiver<Option<ApiResult<CommandOutcome>>>,
}

struct CoalescerInner {
    capacity: usize,
    ttl: Duration,
    completed: HashMap<CoalesceKey, CompletedRecord>,
    order: VecDeque<CoalesceKey>,
    in_flight: HashMap<CoalesceKey, InFlightRecord>,
}

enum BeginStep {
    Replay(CommandOutcome),
    Follower(watch::Receiver<Option<ApiResult<CommandOutcome>>>),
    Leader(LeaderGuard),
}

struct LeaderGuard {
    coalescer: Arc<IdempotencyCoalescer>,
    key: CoalesceKey,
    operation_id: String,
    canonical_body: Vec<u8>,
    cache_completed: bool,
    tx: Option<watch::Sender<Option<ApiResult<CommandOutcome>>>>,
}

impl LeaderGuard {
    fn finish(mut self, result: &ApiResult<CommandOutcome>) {
        if let Ok(mut guard) = self.coalescer.inner.lock() {
            guard.in_flight.remove(&self.key);
            if self.cache_completed
                && let Ok(outcome) = result
            {
                prune_expired_locked(&mut guard);
                if guard.completed.contains_key(&self.key) {
                    if let Some(pos) = guard.order.iter().position(|k| k == &self.key) {
                        let _ = guard.order.remove(pos);
                    }
                } else {
                    while guard.completed.len() >= guard.capacity {
                        if let Some(oldest) = guard.order.pop_front() {
                            guard.completed.remove(&oldest);
                        } else {
                            break;
                        }
                    }
                }
                let expires_at = Instant::now() + guard.ttl;
                guard.order.push_back(self.key.clone());
                guard.completed.insert(
                    self.key.clone(),
                    CompletedRecord {
                        operation_id: self.operation_id.clone(),
                        canonical_body: self.canonical_body.clone(),
                        outcome: outcome.clone(),
                        expires_at,
                    },
                );
            }
        }
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Some(result.clone()));
        }
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            if let Ok(mut guard) = self.coalescer.inner.lock() {
                guard.in_flight.remove(&self.key);
            }
            let _ = tx.send(Some(Err(ApiError::service_unavailable(
                "command_interrupted",
                "in-flight idempotent command was interrupted; retry",
            )
            .with_header("Retry-After", "1"))));
        }
    }
}

/// Bounded in-memory idempotency coalescer and replay table shared by gateway command handlers.
pub struct IdempotencyCoalescer {
    inner: Mutex<CoalescerInner>,
}

impl Default for IdempotencyCoalescer {
    fn default() -> Self {
        Self::new(DEFAULT_IDEMPOTENCY_CAPACITY, DEFAULT_IDEMPOTENCY_TTL)
    }
}

impl IdempotencyCoalescer {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: Mutex::new(CoalescerInner {
                capacity: cap,
                ttl: ttl.max(Duration::from_millis(50)),
                completed: HashMap::with_capacity(cap.min(64)),
                order: VecDeque::with_capacity(cap.min(64)),
                in_flight: HashMap::new(),
            }),
        }
    }

    /// Execute `fut` with in-flight duplicate coalescing and 24-hour completed `2xx` replay caching.
    pub async fn execute<F, Fut>(
        self: &Arc<Self>,
        principal_id: &str,
        scope: &str,
        operation_id: &str,
        idempotency_key: &str,
        canonical_body: &[u8],
        fut: F,
    ) -> ApiResult<CommandOutcome>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ApiResult<CommandOutcome>>,
    {
        self.execute_inner(
            principal_id,
            scope,
            operation_id,
            idempotency_key,
            canonical_body,
            true,
            fut,
        )
        .await
    }

    /// Execute `fut` coalescing concurrent in-flight duplicates while delegating post-completion
    /// replay storage to the backend (useful when backend responses enrich completed state).
    pub async fn execute_inflight_only<F, Fut>(
        self: &Arc<Self>,
        principal_id: &str,
        scope: &str,
        operation_id: &str,
        idempotency_key: &str,
        canonical_body: &[u8],
        fut: F,
    ) -> ApiResult<CommandOutcome>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ApiResult<CommandOutcome>>,
    {
        self.execute_inner(
            principal_id,
            scope,
            operation_id,
            idempotency_key,
            canonical_body,
            false,
            fut,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_inner<F, Fut>(
        self: &Arc<Self>,
        principal_id: &str,
        scope: &str,
        operation_id: &str,
        idempotency_key: &str,
        canonical_body: &[u8],
        cache_completed: bool,
        fut: F,
    ) -> ApiResult<CommandOutcome>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ApiResult<CommandOutcome>>,
    {
        let step = self.begin(
            principal_id,
            scope,
            operation_id,
            idempotency_key,
            canonical_body,
            cache_completed,
        )?;
        match step {
            BeginStep::Replay(outcome) => Ok(outcome),
            BeginStep::Follower(mut rx) => loop {
                if let Some(res) = rx.borrow_and_update().clone() {
                    return match res {
                        Ok(mut outcome) => {
                            outcome.replayed = true;
                            Ok(outcome)
                        }
                        Err(err) => Err(err),
                    };
                }
                if rx.changed().await.is_err() {
                    return Err(ApiError::service_unavailable(
                        "command_interrupted",
                        "in-flight idempotent command was interrupted; retry",
                    )
                    .with_header("Retry-After", "1"));
                }
            },
            BeginStep::Leader(leader) => {
                let res = fut().await;
                leader.finish(&res);
                res
            }
        }
    }

    fn begin(
        self: &Arc<Self>,
        principal_id: &str,
        scope: &str,
        operation_id: &str,
        idempotency_key: &str,
        canonical_body: &[u8],
        cache_completed: bool,
    ) -> ApiResult<BeginStep> {
        let mut guard = self.inner.lock().map_err(|_| {
            ApiError::internal("state_poisoned", "idempotency coalescer mutex poisoned")
        })?;
        prune_expired_locked(&mut guard);

        let key = CoalesceKey {
            principal_id: principal_id.to_owned(),
            scope: scope.to_owned(),
            idempotency_key: idempotency_key.to_owned(),
        };

        if let Some(done) = guard.completed.get(&key) {
            if done.operation_id == operation_id && done.canonical_body == canonical_body {
                let mut replayed = done.outcome.clone();
                replayed.replayed = true;
                return Ok(BeginStep::Replay(replayed));
            }
            return Err(build_conflict_error(scope, Some(&done.outcome)));
        }

        if let Some(inflight) = guard.in_flight.get(&key) {
            if inflight.operation_id == operation_id && inflight.canonical_body == canonical_body {
                return Ok(BeginStep::Follower(inflight.rx.clone()));
            }
            return Err(build_conflict_error(scope, None));
        }

        let (tx, rx) = watch::channel(None);
        guard.in_flight.insert(
            key.clone(),
            InFlightRecord {
                operation_id: operation_id.to_owned(),
                canonical_body: canonical_body.to_vec(),
                rx,
            },
        );
        Ok(BeginStep::Leader(LeaderGuard {
            coalescer: Arc::clone(self),
            key,
            operation_id: operation_id.to_owned(),
            canonical_body: canonical_body.to_vec(),
            cache_completed,
            tx: Some(tx),
        }))
    }
}

fn build_conflict_error(scope: &str, prior: Option<&CommandOutcome>) -> ApiError {
    let mut err = ApiError::conflict(
        "idempotency_conflict",
        "Idempotency-Key reused with different request payload",
    )
    .with_retryable(false)
    .with_error_code_header();
    if scope == NETWORK_MUTATION_SCOPE {
        let orig_tx = prior
            .and_then(|o| o.body.as_ref())
            .and_then(|b| b.get("transaction"))
            .and_then(|t| t.get("transaction_id"))
            .and_then(Value::as_str);
        let details = match orig_tx {
            Some(tx_id) => json!({
                "idempotency_scope": NETWORK_MUTATION_SCOPE,
                "original_transaction_id": tx_id,
            }),
            None => json!({
                "idempotency_scope": NETWORK_MUTATION_SCOPE,
            }),
        };
        err = err.with_details(details);
    }
    err
}

fn prune_expired_locked(guard: &mut CoalescerInner) {
    let now = Instant::now();
    while let Some(front) = guard.order.front() {
        let expired = guard
            .completed
            .get(front)
            .is_none_or(|rec| rec.expires_at <= now);
        if expired {
            if let Some(k) = guard.order.pop_front() {
                guard.completed.remove(&k);
            }
        } else {
            break;
        }
    }
}
