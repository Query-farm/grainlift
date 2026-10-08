// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use adbc_core::CancelHandle;
use adbc_core::error::{Error as AdbcError, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use uuid::Uuid;

use crate::backend::{
    Backend, BackendConnection, BackendStatement, ErasedProducer, ProducerDecoder, QueryResult,
    ResultSource,
};
use crate::bind_upload::BindUpload;
use crate::config::{ClientOptionPolicy, TargetConfig};

/// Default bound on the encoded [`ResultProducer`](crate::backend::ResultProducer)
/// state carried in each continuation token.
pub const DEFAULT_PRODUCER_STATE_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct SessionLimits {
    pub max_sessions: usize,
    pub max_sessions_per_principal: usize,
    pub max_statements_per_session: usize,
    pub max_results_per_session: usize,
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            max_sessions_per_principal: 32,
            max_statements_per_session: 64,
            max_results_per_session: 64,
        }
    }
}

impl SessionLimits {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_sessions == 0
            || self.max_sessions_per_principal == 0
            || self.max_statements_per_session == 0
            || self.max_results_per_session == 0
        {
            return Err("all session, statement, and result limits must be positive");
        }
        if self.max_sessions_per_principal > self.max_sessions {
            return Err("max_sessions_per_principal must not exceed max_sessions");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct TargetAuthorizer {
    permissions: Option<HashMap<String, HashSet<String>>>,
}

impl TargetAuthorizer {
    pub fn new(permissions: HashMap<String, Vec<String>>) -> Self {
        if permissions.is_empty() {
            return Self::default();
        }
        Self {
            permissions: Some(
                permissions
                    .into_iter()
                    .map(|(principal, targets)| (principal, targets.into_iter().collect()))
                    .collect(),
            ),
        }
    }

    fn allows(&self, principal: &str, target: &str) -> bool {
        let Some(permissions) = &self.permissions else {
            return true;
        };
        let principal_name = principal
            .split_once('\0')
            .map_or(principal, |(_, name)| name);
        permissions
            .get(principal_name)
            .or_else(|| permissions.get(principal))
            .is_some_and(|targets| targets.contains("*") || targets.contains(target))
    }
}

pub struct SessionManager {
    partition_signer: crate::partitions::PartitionSigner,
    backend: Arc<dyn Backend>,
    targets: HashMap<String, TargetConfig>,
    registry: Mutex<SessionRegistry>,
    ttl: Duration,
    require_authentication: bool,
    limits: SessionLimits,
    authorizer: TargetAuthorizer,
    operation_timeout: Duration,
    producer_state_bytes: usize,
    closing: AtomicBool,
}

#[derive(Default)]
struct SessionRegistry {
    sessions: HashMap<String, Arc<Session>>,
    // Active transport IDs are server-generated and bounded by listener
    // connection admission. Closed IDs are removed, never kept as tombstones.
    transports: HashMap<String, TransportOwner>,
    opening_total: usize,
    opening_by_principal: HashMap<String, usize>,
}

struct TransportOwner {
    principal: String,
    access: Option<TransportTargetAccess>,
}

/// Server-owned grants attached to an admitted physical connection, never
/// accepted from RPC input or credential claims.
pub(crate) struct TransportTargetAccess {
    pub public_targets: Arc<HashSet<String>>,
    pub allow_principal_targets: bool,
}

pub struct Session {
    principal: String,
    transport_id: Option<String>,
    target: String,
    last_used: Mutex<Instant>,
    resources: Arc<SessionResources>,
    worker: SessionWorker,
    connection_cancel: Arc<dyn CancelHandle>,
    connection_option_policy: ClientOptionPolicy,
    bind_uploads: Mutex<HashMap<String, BindUploadEntry>>,
    limits: SessionLimits,
    producer_state_bytes: usize,
}

struct SessionResources {
    connection: Mutex<Box<dyn BackendConnection>>,
    statements: Mutex<HashMap<String, Arc<StatementEntry>>>,
    results: Mutex<HashMap<String, Arc<Mutex<ResultEntry>>>>,
}

type WorkerJob = Box<dyn FnOnce() + Send + 'static>;

struct SessionWorker {
    tx: SyncSender<WorkerJob>,
    timeout: Duration,
}

impl SessionWorker {
    fn start(timeout: Duration) -> Result<Self, AdbcError> {
        let (tx, rx) = mpsc::sync_channel::<WorkerJob>(32);
        thread::Builder::new()
            .name("grainlift-session".to_string())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    job();
                }
            })
            .map_err(|error| internal(format!("start session worker: {error}")))?;
        Ok(Self { tx, timeout })
    }

    fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce() -> Result<T, AdbcError> + Send + 'static,
    ) -> Result<T, AdbcError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let abandoned = Arc::new(AtomicBool::new(false));
        let job_abandoned = Arc::clone(&abandoned);
        let job = Box::new(move || {
            if job_abandoned.load(Ordering::Acquire) {
                return;
            }
            let _ = reply_tx.send(operation());
        });
        match self.tx.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => return Err(busy("session worker queue is full")),
            Err(TrySendError::Disconnected(_)) => {
                return Err(internal("session worker stopped"));
            }
        }
        match reply_rx.recv_timeout(self.timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                abandoned.store(true, Ordering::Release);
                Err(timeout(format!(
                    "downstream operation exceeded {:?}",
                    self.timeout
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(internal("session worker stopped")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindMode {
    Batch,
    Stream,
}

struct BindUploadEntry {
    statement_id: String,
    mode: BindMode,
    upload: BindUpload,
}

pub struct StatementEntry {
    pub statement: Mutex<Box<dyn BackendStatement>>,
    pub cancel: Arc<dyn CancelHandle>,
}

pub struct ResultEntry {
    owner_statement_id: Option<String>,
    source: EntrySource,
    schema: SchemaRef,
    next_sequence: i64,
    finished: bool,
}

enum EntrySource {
    /// A live reader plus the last batch, retained for replay.
    Reader {
        reader: Box<dyn RecordBatchReader + Send + 'static>,
        last: Option<(i64, RecordBatch)>,
        terminal_error: Option<(i64, AdbcError)>,
    },
    /// Only the initial encoded state; later state lives in continuation
    /// tokens, so no iterator or replay batch is retained.
    Producer {
        decode: ProducerDecoder,
        initial_state: Vec<u8>,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResourceCounts {
    pub sessions: usize,
    pub statements: usize,
    pub results: usize,
    pub bind_uploads: usize,
    pub opening_sessions: usize,
}

impl SessionManager {
    /// Construct a manager with production-safe default quotas and no target
    /// restrictions. `with_limits_and_authorizer` is used by the service
    /// binary to apply its validated configuration.
    pub fn new(
        backend: Arc<dyn Backend>,
        targets: HashMap<String, TargetConfig>,
        ttl: Duration,
        require_authentication: bool,
    ) -> Self {
        Self::with_limits_and_authorizer(
            backend,
            targets,
            ttl,
            require_authentication,
            SessionLimits::default(),
            TargetAuthorizer::default(),
        )
    }

    pub fn with_limits_and_authorizer(
        backend: Arc<dyn Backend>,
        targets: HashMap<String, TargetConfig>,
        ttl: Duration,
        require_authentication: bool,
        limits: SessionLimits,
        authorizer: TargetAuthorizer,
    ) -> Self {
        Self::with_limits_authorizer_and_timeout(
            backend,
            targets,
            ttl,
            require_authentication,
            limits,
            authorizer,
            Duration::from_secs(300),
        )
    }

    pub fn with_limits_authorizer_and_timeout(
        backend: Arc<dyn Backend>,
        targets: HashMap<String, TargetConfig>,
        ttl: Duration,
        require_authentication: bool,
        limits: SessionLimits,
        authorizer: TargetAuthorizer,
        operation_timeout: Duration,
    ) -> Self {
        debug_assert!(limits.validate().is_ok());
        debug_assert!(!operation_timeout.is_zero());
        Self {
            partition_signer: crate::partitions::PartitionSigner::new(ttl),
            backend,
            targets,
            registry: Mutex::new(SessionRegistry::default()),
            ttl,
            require_authentication,
            limits,
            authorizer,
            operation_timeout,
            producer_state_bytes: DEFAULT_PRODUCER_STATE_BYTES,
            closing: AtomicBool::new(false),
        }
    }

    /// Bound the encoded [`ResultProducer`](crate::backend::ResultProducer)
    /// state carried in each continuation token (default
    /// [`DEFAULT_PRODUCER_STATE_BYTES`]). Larger state fails execution or the
    /// fetch with ADBC `INVALID_DATA`.
    pub fn with_producer_state_limit(mut self, bytes: usize) -> Self {
        debug_assert!(bytes > 0);
        self.producer_state_bytes = bytes;
        self
    }

    pub fn principal(&self, auth: &vgi_rpc::AuthContext) -> vgi_rpc::Result<String> {
        if self.require_authentication {
            auth.require_authenticated()?;
        }
        if auth.authenticated {
            Ok(format!("{}\0{}", auth.domain, auth.principal))
        } else {
            Ok("\0anonymous".to_string())
        }
    }

    pub fn seal_partition(
        &self,
        session: &Session,
        descriptor: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<u8>, AdbcError> {
        self.partition_signer
            .seal(&session.target, &session.principal, descriptor, limit)
    }

    pub fn open_partition(
        &self,
        session: &Session,
        token: &[u8],
        limit: usize,
    ) -> Result<Vec<u8>, AdbcError> {
        self.partition_signer
            .open(&session.target, &session.principal, token, limit)
    }

    pub fn open(
        &self,
        principal: String,
        target_name: &str,
        database_options: Vec<(String, adbc_core::options::OptionValue)>,
        connection_options: Vec<(String, adbc_core::options::OptionValue)>,
    ) -> Result<String, AdbcError> {
        self.open_on_transport(
            principal,
            target_name,
            database_options,
            connection_options,
            None,
        )
    }

    pub fn open_on_transport(
        &self,
        principal: String,
        target_name: &str,
        database_options: Vec<(String, adbc_core::options::OptionValue)>,
        connection_options: Vec<(String, adbc_core::options::OptionValue)>,
        transport_id: Option<&str>,
    ) -> Result<String, AdbcError> {
        let allowed = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| internal("session registry is poisoned"))?;
            Self::validate_transport(&registry, transport_id, &principal)?;
            let access = transport_id
                .and_then(|id| registry.transports.get(id))
                .and_then(|owner| owner.access.as_ref());
            match access {
                Some(access) => {
                    access.public_targets.contains(target_name)
                        || (access.allow_principal_targets
                            && self.authorizer.allows(&principal, target_name))
                }
                None => self.authorizer.allows(&principal, target_name),
            }
        };
        if !allowed {
            return Err(AdbcError::with_message_and_status(
                "principal is not authorized for the requested target",
                Status::Unauthorized,
            ));
        }
        let target = self.targets.get(target_name).cloned().ok_or_else(|| {
            AdbcError::with_message_and_status("target is not configured", Status::NotFound)
        })?;
        let connection_option_policy = target.connection_option_policy();

        self.reserve_open(&principal, transport_id)?;
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let backend = Arc::clone(&self.backend);
        let spawn = thread::Builder::new()
            .name("grainlift-open".to_string())
            .spawn(move || {
                let result = backend.open(&target, database_options, connection_options);
                let _ = reply_tx.send(result);
            });
        let connection = match spawn {
            Ok(_) => match reply_rx.recv_timeout(self.operation_timeout) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => Err(timeout(format!(
                    "downstream connection open exceeded {:?}",
                    self.operation_timeout
                ))),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    Err(internal("connection open worker stopped"))
                }
            },
            Err(error) => Err(internal(format!("start connection open worker: {error}"))),
        };

        let connection = match connection {
            Ok(connection) => connection,
            Err(error) => {
                self.release_open_reservation(&principal)?;
                return Err(error);
            }
        };
        let connection_cancel = connection.cancel_handle();
        let id = Uuid::new_v4().to_string();
        let resources = Arc::new(SessionResources {
            connection: Mutex::new(connection),
            statements: Mutex::new(HashMap::new()),
            results: Mutex::new(HashMap::new()),
        });
        let worker = match SessionWorker::start(self.operation_timeout) {
            Ok(worker) => worker,
            Err(error) => {
                self.release_open_reservation(&principal)?;
                let _ = thread::Builder::new()
                    .name("grainlift-failed-open-cleanup".to_string())
                    .spawn(move || drop(resources));
                return Err(error);
            }
        };
        let session = Arc::new(Session {
            principal: principal.clone(),
            transport_id: transport_id.map(str::to_owned),
            target: target_name.to_string(),
            last_used: Mutex::new(Instant::now()),
            resources,
            worker,
            connection_cancel,
            connection_option_policy,
            bind_uploads: Mutex::new(HashMap::new()),
            limits: self.limits,
            producer_state_bytes: self.producer_state_bytes,
        });
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        Self::release_open_reservation_locked(&mut registry, &principal);
        if self.closing.load(Ordering::Acquire) {
            drop(registry);
            drop(session);
            return Err(busy("proxy is shutting down"));
        }
        if let Err(error) = Self::validate_transport(&registry, transport_id, &principal) {
            drop(registry);
            drop(session);
            return Err(error);
        }
        registry.sessions.insert(id.clone(), session);
        Ok(id)
    }

    fn validate_transport(
        registry: &SessionRegistry,
        transport_id: Option<&str>,
        principal: &str,
    ) -> Result<(), AdbcError> {
        if let Some(id) = transport_id
            && registry
                .transports
                .get(id)
                .map(|owner| owner.principal.as_str())
                != Some(principal)
        {
            return Err(not_found("closed or unavailable transport connection"));
        }
        Ok(())
    }

    /// Register a physical transport after authentication and admission.
    pub fn open_transport(&self, principal: String) -> Result<String, AdbcError> {
        self.open_transport_with_access(principal, None)
    }

    pub(crate) fn open_transport_with_access(
        &self,
        principal: String,
        access: Option<TransportTargetAccess>,
    ) -> Result<String, AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        if self.closing.load(Ordering::Acquire) {
            return Err(busy("proxy is shutting down"));
        }
        let id = Uuid::new_v4().to_string();
        registry
            .transports
            .insert(id.clone(), TransportOwner { principal, access });
        Ok(id)
    }

    /// Revoke this connection's sessions without affecting other connections
    /// owned by the same principal. Slow opens cannot register after removal.
    pub fn close_transport(&self, id: &str) -> Result<usize, AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        registry.transports.remove(id);
        let ids = registry
            .sessions
            .iter()
            .filter(|(_, session)| session.transport_id.as_deref() == Some(id))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let removed = ids
            .iter()
            .filter_map(|id| registry.sessions.remove(id))
            .collect::<Vec<_>>();
        drop(registry);
        for session in removed {
            // Native cancellation can block. Revocation is immediate; driver
            // cleanup remains off the listener and registry threads.
            if Arc::strong_count(&session) > 1 {
                let cancel = Arc::clone(&session.connection_cancel);
                let _ = thread::Builder::new()
                    .name("grainlift-disconnect-cancel".into())
                    .spawn(move || {
                        let _ = cancel.try_cancel();
                    });
            }
            drop(session);
        }
        Ok(ids.len())
    }

    fn release_open_reservation(&self, principal: &str) -> Result<(), AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        Self::release_open_reservation_locked(&mut registry, principal);
        Ok(())
    }

    fn release_open_reservation_locked(registry: &mut SessionRegistry, principal: &str) {
        registry.opening_total = registry.opening_total.saturating_sub(1);
        if let Some(opening) = registry.opening_by_principal.get_mut(principal) {
            *opening = opening.saturating_sub(1);
            if *opening == 0 {
                registry.opening_by_principal.remove(principal);
            }
        }
    }

    fn reserve_open(&self, principal: &str, transport_id: Option<&str>) -> Result<(), AdbcError> {
        if self.closing.load(Ordering::Acquire) {
            return Err(busy("proxy is shutting down"));
        }
        // Reaping before admission ensures dead leases do not consume quota
        // until the next background interval.
        self.reap_expired()?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        if self.closing.load(Ordering::Acquire) {
            return Err(busy("proxy is shutting down"));
        }
        Self::validate_transport(&registry, transport_id, principal)?;
        let total = registry.sessions.len() + registry.opening_total;
        if total >= self.limits.max_sessions {
            return Err(quota("global session"));
        }
        let active_for_principal = registry
            .sessions
            .values()
            .filter(|session| session.principal == principal)
            .count();
        let opening_for_principal = registry
            .opening_by_principal
            .get(principal)
            .copied()
            .unwrap_or_default();
        if active_for_principal + opening_for_principal >= self.limits.max_sessions_per_principal {
            return Err(quota("per-principal session"));
        }
        registry.opening_total += 1;
        *registry
            .opening_by_principal
            .entry(principal.to_string())
            .or_default() += 1;
        Ok(())
    }

    pub fn get(&self, id: &str, principal: &str) -> Result<Arc<Session>, AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let session = registry
            .sessions
            .get(id)
            .ok_or_else(|| not_found("session"))?;
        if session.principal != principal {
            return Err(AdbcError::with_message_and_status(
                "session does not belong to the authenticated principal",
                Status::Unauthorized,
            ));
        }
        let mut last_used = session
            .last_used
            .lock()
            .map_err(|_| internal("session lease is poisoned"))?;
        // The registry owns one strong reference. Any additional reference
        // denotes an operation that already acquired the session. A lease is
        // an idle timeout, so a new request may refresh an otherwise-expired
        // session while prior work is still in flight.
        let already_in_flight = Arc::strong_count(session) > 1;
        if last_used.elapsed() > self.ttl && !already_in_flight {
            drop(last_used);
            let removed = registry.sessions.remove(id);
            drop(registry);
            drop(removed);
            return Err(not_found("expired session"));
        }
        *last_used = Instant::now();
        drop(last_used);
        Ok(Arc::clone(session))
    }

    pub fn close(&self, id: &str, principal: &str) -> Result<(), AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let session = registry
            .sessions
            .get(id)
            .ok_or_else(|| not_found("session"))?;
        if session.principal != principal {
            return Err(AdbcError::with_message_and_status(
                "session does not belong to the authenticated principal",
                Status::Unauthorized,
            ));
        }
        let removed = registry.sessions.remove(id);
        drop(registry);
        drop(removed);
        Ok(())
    }

    /// Remove expired idle leases. A session with an in-flight operation is
    /// retained even when the wall-clock TTL has elapsed, then becomes
    /// eligible after the final active reference is released.
    pub fn reap_expired(&self) -> Result<usize, AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let mut expired = Vec::new();
        for (id, session) in &registry.sessions {
            // Do not expire a lease while a handler holds the session. The
            // operation may run longer than the idle TTL; it becomes eligible
            // as soon as the final in-flight reference is released.
            if Arc::strong_count(session) > 1 {
                continue;
            }
            let last_used = session
                .last_used
                .lock()
                .map_err(|_| internal("session lease is poisoned"))?;
            if last_used.elapsed() > self.ttl {
                expired.push(id.clone());
            }
        }
        let removed: Vec<_> = expired
            .iter()
            .filter_map(|id| registry.sessions.remove(id))
            .collect();
        drop(registry);
        drop(removed);
        Ok(expired.len())
    }

    /// Detach every session during graceful shutdown. Active calls keep their
    /// resources until they return; all idle resources are dropped now.
    pub fn close_all(&self) -> Result<usize, AdbcError> {
        self.closing.store(true, Ordering::Release);
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let sessions = std::mem::take(&mut registry.sessions);
        registry.transports.clear();
        let count = sessions.len();
        drop(registry);
        drop(sessions);
        Ok(count)
    }

    /// Return aggregate handle counts without exposing principals or handle
    /// identifiers. This is suitable for bounded-resource telemetry and
    /// lifecycle assertions.
    pub fn resource_counts(&self) -> Result<ResourceCounts, AdbcError> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let mut counts = ResourceCounts {
            sessions: registry.sessions.len(),
            opening_sessions: registry.opening_total,
            ..ResourceCounts::default()
        };
        for session in registry.sessions.values() {
            counts.statements += session
                .resources
                .statements
                .lock()
                .map_err(|_| internal("statement registry is poisoned"))?
                .len();
            counts.results += session
                .resources
                .results
                .lock()
                .map_err(|_| internal("result registry is poisoned"))?
                .len();
            counts.bind_uploads += session
                .bind_uploads
                .lock()
                .map_err(|_| internal("bind upload registry is poisoned"))?
                .len();
        }
        Ok(counts)
    }

    #[cfg(test)]
    fn session_count(&self) -> usize {
        self.registry
            .lock()
            .expect("session registry")
            .sessions
            .len()
    }
}

impl Session {
    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut dyn BackendConnection) -> Result<T, AdbcError> + Send + 'static,
    ) -> Result<T, AdbcError>
    where
        T: Send + 'static,
    {
        let resources = Arc::clone(&self.resources);
        self.worker.call(move || {
            let mut connection = resources
                .connection
                .lock()
                .map_err(|_| internal("connection is poisoned"))?;
            operation(connection.as_mut())
        })
    }

    pub fn set_connection_option(
        &self,
        key: String,
        value: adbc_core::options::OptionValue,
    ) -> Result<(), AdbcError> {
        if !self.connection_option_policy.permits(&key) {
            let reason = if self.connection_option_policy.is_protected(&key) {
                "is controlled by the proxy server"
            } else {
                "is not allowed by the target policy"
            };
            return Err(AdbcError::with_message_and_status(
                format!("client connection option {key:?} {reason}"),
                Status::InvalidArguments,
            ));
        }
        self.with_connection(move |connection| connection.set_option(&key, value))
    }

    pub fn cancel_connection(&self) -> Result<(), AdbcError> {
        self.connection_cancel.try_cancel()
    }

    pub fn commit(&self) -> Result<(), AdbcError> {
        self.with_connection(|connection| connection.commit())
    }

    pub fn rollback(&self) -> Result<(), AdbcError> {
        self.with_connection(|connection| connection.rollback())
    }

    pub fn new_statement(&self) -> Result<String, AdbcError> {
        let resources = Arc::clone(&self.resources);
        let limit = self.limits.max_statements_per_session;
        self.worker.call(move || {
            let mut statements = resources
                .statements
                .lock()
                .map_err(|_| internal("statement registry is poisoned"))?;
            if statements.len() >= limit {
                return Err(quota("statement"));
            }
            let statement = resources
                .connection
                .lock()
                .map_err(|_| internal("connection is poisoned"))?
                .new_statement()?;
            let cancel = statement.cancel_handle();
            let id = Uuid::new_v4().to_string();
            statements.insert(
                id.clone(),
                Arc::new(StatementEntry {
                    statement: Mutex::new(statement),
                    cancel,
                }),
            );
            Ok(id)
        })
    }

    pub fn with_statement<T>(
        &self,
        id: &str,
        operation: impl FnOnce(&mut dyn BackendStatement) -> Result<T, AdbcError> + Send + 'static,
    ) -> Result<T, AdbcError>
    where
        T: Send + 'static,
    {
        let resources = Arc::clone(&self.resources);
        let id = id.to_string();
        self.worker.call(move || {
            let statement = resources
                .statements
                .lock()
                .map_err(|_| internal("statement registry is poisoned"))?
                .get(&id)
                .cloned()
                .ok_or_else(|| not_found("statement"))?;
            let mut statement = statement
                .statement
                .lock()
                .map_err(|_| internal("statement is poisoned"))?;
            operation(statement.as_mut())
        })
    }

    pub fn cancel_statement(&self, id: &str) -> Result<(), AdbcError> {
        self.resources
            .statements
            .lock()
            .map_err(|_| internal("statement registry is poisoned"))?
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("statement"))?
            .cancel
            .try_cancel()
    }

    pub fn close_statement(&self, id: &str) -> Result<(), AdbcError> {
        let resources = Arc::clone(&self.resources);
        let statement_id = id.to_string();
        self.worker.call(move || {
            resources
                .statements
                .lock()
                .map_err(|_| internal("statement registry is poisoned"))?
                .remove(&statement_id)
                .ok_or_else(|| not_found("statement"))?;
            Ok(())
        })?;
        self.invalidate_statement_results(id)?;
        let uploads = {
            let mut registry = self
                .bind_uploads
                .lock()
                .map_err(|_| internal("bind upload registry is poisoned"))?;
            let ids = registry
                .iter()
                .filter(|(_, entry)| entry.statement_id == id)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| registry.remove(&id))
                .collect::<Vec<_>>()
        };
        for entry in uploads {
            entry.upload.cancel();
        }
        Ok(())
    }

    pub fn start_bind_upload(
        &self,
        statement_id: &str,
        mode: BindMode,
        schema: SchemaRef,
        max_bytes: usize,
    ) -> Result<String, AdbcError> {
        if !self
            .resources
            .statements
            .lock()
            .map_err(|_| internal("statement registry is poisoned"))?
            .contains_key(statement_id)
        {
            return Err(not_found("statement"));
        }
        let upload = BindUpload::start(schema, max_bytes)?;
        let id = Uuid::new_v4().to_string();
        self.bind_uploads
            .lock()
            .map_err(|_| internal("bind upload registry is poisoned"))?
            .insert(
                id.clone(),
                BindUploadEntry {
                    statement_id: statement_id.to_string(),
                    mode,
                    upload,
                },
            );
        Ok(id)
    }

    pub fn push_bind_upload(&self, id: &str, batch: RecordBatch) -> Result<(), AdbcError> {
        self.bind_uploads
            .lock()
            .map_err(|_| internal("bind upload registry is poisoned"))?
            .get(id)
            .ok_or_else(|| not_found("bind upload"))?
            .upload
            .push(batch)
    }

    pub fn finish_bind_upload(&self, id: &str) -> Result<(), AdbcError> {
        let entry = self
            .bind_uploads
            .lock()
            .map_err(|_| internal("bind upload registry is poisoned"))?
            .remove(id)
            .ok_or_else(|| not_found("bind upload"))?;
        let mut reader = entry.upload.finish()?;
        match entry.mode {
            BindMode::Batch => {
                let batch = reader
                    .next()
                    .ok_or_else(|| invalid_data("bind requires exactly one record batch"))?
                    .map_err(AdbcError::from)?;
                if reader.next().is_some() {
                    return Err(invalid_data("bind requires exactly one record batch"));
                }
                self.with_statement(&entry.statement_id, move |statement| statement.bind(batch))
            }
            BindMode::Stream => self.with_statement(&entry.statement_id, move |statement| {
                statement.bind_stream(reader)
            }),
        }
    }

    pub fn cancel_bind_upload(&self, id: &str) {
        if let Ok(Some(entry)) = self
            .bind_uploads
            .lock()
            .map(|mut uploads| uploads.remove(id))
        {
            entry.upload.cancel();
        }
    }

    pub fn insert_result(
        &self,
        reader: Box<dyn RecordBatchReader + Send + 'static>,
    ) -> Result<(String, SchemaRef), AdbcError> {
        self.insert_owned_result(None, QueryResult::from_reader(reader))
    }

    pub fn insert_statement_result(
        &self,
        statement_id: &str,
        reader: Box<dyn RecordBatchReader + Send + 'static>,
    ) -> Result<(String, SchemaRef), AdbcError> {
        self.insert_statement_query_result(statement_id, QueryResult::from_reader(reader))
    }

    /// Register a statement's result, replacing its earlier results. A
    /// producer's initial state is encoded and size-checked now.
    pub fn insert_statement_query_result(
        &self,
        statement_id: &str,
        result: QueryResult,
    ) -> Result<(String, SchemaRef), AdbcError> {
        self.invalidate_statement_results(statement_id)?;
        self.insert_owned_result(Some(statement_id.to_string()), result)
    }

    pub fn invalidate_statement_results(&self, statement_id: &str) -> Result<(), AdbcError> {
        let resources = Arc::clone(&self.resources);
        let statement_id = statement_id.to_string();
        self.worker.call(move || {
            resources
                .results
                .lock()
                .map_err(|_| internal("result registry is poisoned"))?
                .retain(|_, result| {
                    result
                        .lock()
                        .map(|entry| entry.owner_statement_id.as_deref() != Some(&statement_id))
                        .unwrap_or(false)
                });
            Ok(())
        })
    }

    fn insert_owned_result(
        &self,
        owner_statement_id: Option<String>,
        result: QueryResult,
    ) -> Result<(String, SchemaRef), AdbcError> {
        let resources = Arc::clone(&self.resources);
        let limit = self.limits.max_results_per_session;
        let state_limit = self.producer_state_bytes;
        self.worker.call(move || {
            let (schema, source) = result.into_parts();
            let source = match source {
                ResultSource::Reader(reader) => EntrySource::Reader {
                    reader,
                    last: None,
                    terminal_error: None,
                },
                ResultSource::Producer(producer, decode) => EntrySource::Producer {
                    decode,
                    initial_state: encode_producer(producer.as_ref(), state_limit)?,
                },
            };
            let id = Uuid::new_v4().to_string();
            let mut results = resources
                .results
                .lock()
                .map_err(|_| internal("result registry is poisoned"))?;
            if results.len() >= limit {
                return Err(quota("result"));
            }
            results.insert(
                id.clone(),
                Arc::new(Mutex::new(ResultEntry {
                    owner_statement_id,
                    source,
                    schema: schema.clone(),
                    next_sequence: 0,
                    finished: false,
                })),
            );
            Ok((id, schema))
        })
    }

    pub fn result_schema(&self, id: &str) -> Result<SchemaRef, AdbcError> {
        let resources = Arc::clone(&self.resources);
        let id = id.to_string();
        self.worker.call(move || {
            let result = resources
                .results
                .lock()
                .map_err(|_| internal("result registry is poisoned"))?
                .get(&id)
                .cloned()
                .ok_or_else(|| not_found("result"))?;
            let schema = result
                .lock()
                .map_err(|_| internal("result is poisoned"))?
                .schema();
            Ok(schema)
        })
    }

    /// Open a `read_result` stream: the result schema and, for a producer
    /// result, its initial encoded state. Producer results resume only from
    /// continuation tokens, so they accept only sequence 0 here.
    pub fn open_result_stream(
        &self,
        id: &str,
        sequence: i64,
    ) -> Result<(SchemaRef, Option<Vec<u8>>), AdbcError> {
        let resources = Arc::clone(&self.resources);
        let id = id.to_string();
        self.worker.call(move || {
            let result = resources
                .results
                .lock()
                .map_err(|_| internal("result registry is poisoned"))?
                .get(&id)
                .cloned()
                .ok_or_else(|| not_found("result"))?;
            let entry = result.lock().map_err(|_| internal("result is poisoned"))?;
            match &entry.source {
                EntrySource::Reader { .. } => Ok((entry.schema(), None)),
                EntrySource::Producer { initial_state, .. } if sequence == 0 => {
                    Ok((entry.schema(), Some(initial_state.clone())))
                }
                EntrySource::Producer { .. } => Err(AdbcError::with_message_and_status(
                    "Producer results resume from continuation tokens",
                    Status::InvalidArguments,
                )),
            }
        })
    }

    /// Resume a producer result from continuation-token `state`, produce the
    /// batch at `sequence`, and return it with the state for the next token.
    ///
    /// Fetching the previous sequence again re-produces its batch from the
    /// token's state; older sequences fail. Batches must match the result
    /// schema and stay within `max_batch_bytes`. Any production, validation or
    /// encoding failure closes the result.
    pub fn next_produced_result(
        &self,
        id: &str,
        sequence: i64,
        state: Vec<u8>,
        max_batch_bytes: usize,
    ) -> Result<(Option<RecordBatch>, Vec<u8>), AdbcError> {
        let resources = Arc::clone(&self.resources);
        let id = id.to_string();
        let state_limit = self.producer_state_bytes;
        self.worker.call(move || {
            let result = resources
                .results
                .lock()
                .map_err(|_| internal("result registry is poisoned"))?
                .get(&id)
                .cloned()
                .ok_or_else(|| not_found("result"))?;
            let mut entry = result.lock().map_err(|_| internal("result is poisoned"))?;
            let EntrySource::Producer { decode, .. } = &entry.source else {
                return Err(not_found("producer result"));
            };
            let decode = *decode;
            if sequence != entry.next_sequence && sequence != entry.next_sequence - 1 {
                return Err(AdbcError::with_message_and_status(
                    "Invalid result sequence",
                    Status::InvalidArguments,
                ));
            }
            if entry.finished && sequence == entry.next_sequence {
                return Ok((None, state));
            }
            match produce_from_state(decode, &state, &entry.schema, max_batch_bytes, state_limit) {
                Ok(None) => {
                    entry.finished = true;
                    Ok((None, state))
                }
                Ok(Some((batch, advanced))) => {
                    entry.next_sequence = entry.next_sequence.max(sequence + 1);
                    Ok((Some(batch), advanced))
                }
                Err(error) => {
                    drop(entry);
                    let removed = resources
                        .results
                        .lock()
                        .map_err(|_| internal("result registry is poisoned"))?
                        .remove(&id);
                    drop(removed);
                    Err(error)
                }
            }
        })
    }

    pub fn next_result(&self, id: &str, sequence: i64) -> Result<Option<RecordBatch>, AdbcError> {
        let resources = Arc::clone(&self.resources);
        let id = id.to_string();
        self.worker.call(move || {
            let result = resources
                .results
                .lock()
                .map_err(|_| internal("result registry is poisoned"))?
                .get(&id)
                .cloned()
                .ok_or_else(|| not_found("result"))?;
            result
                .lock()
                .map_err(|_| internal("result is poisoned"))?
                .next(sequence)
        })
    }

    pub fn close_result(&self, id: &str) -> Result<(), AdbcError> {
        let resources = Arc::clone(&self.resources);
        let id = id.to_string();
        self.worker.call(move || {
            resources
                .results
                .lock()
                .map_err(|_| internal("result registry is poisoned"))?
                .remove(&id)
                .ok_or_else(|| not_found("result"))?;
            Ok(())
        })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let connection_cancel = Arc::clone(&self.connection_cancel);
        let resources = Arc::clone(&self.resources);
        let statement_cancels = self
            .resources
            .statements
            .lock()
            .map(|statements| {
                statements
                    .values()
                    .map(|statement| Arc::clone(&statement.cancel))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Ok(mut uploads) = self.bind_uploads.lock() {
            for (_, entry) in uploads.drain() {
                entry.upload.cancel();
            }
        }
        // Cancellation and native handle destructors are FFI and may block.
        // Keep both off the registry/shutdown thread; a process supervisor is
        // the ultimate hard deadline for a driver that ignores cancellation.
        let _ = thread::Builder::new()
            .name("grainlift-session-cleanup".to_string())
            .spawn(move || {
                let _ = connection_cancel.try_cancel();
                for cancel in statement_cancels {
                    let _ = cancel.try_cancel();
                }
                drop(resources);
            });
    }
}

impl ResultEntry {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn next(&mut self, sequence: i64) -> Result<Option<RecordBatch>, AdbcError> {
        let EntrySource::Reader {
            reader,
            last,
            terminal_error,
        } = &mut self.source
        else {
            return Err(AdbcError::with_message_and_status(
                "Producer results resume from continuation tokens",
                Status::InvalidArguments,
            ));
        };
        if let Some((last_sequence, batch)) = last
            && sequence == *last_sequence
        {
            return Ok(Some(batch.clone()));
        }
        if let Some((error_sequence, error)) = terminal_error
            && sequence == *error_sequence
        {
            return Err(error.clone());
        }
        if sequence != self.next_sequence {
            return Err(AdbcError::with_message_and_status(
                format!(
                    "invalid result sequence: expected {}, got {sequence}",
                    self.next_sequence
                ),
                Status::InvalidState,
            ));
        }
        if self.finished {
            return Ok(None);
        }
        match reader.next() {
            Some(Ok(batch)) => {
                *last = Some((sequence, batch.clone()));
                self.next_sequence += 1;
                Ok(Some(batch))
            }
            Some(Err(error)) => {
                let error = AdbcError::from(error);
                *terminal_error = Some((sequence, error.clone()));
                self.finished = true;
                Err(error)
            }
            None => {
                self.finished = true;
                *last = None;
                Ok(None)
            }
        }
    }
}

fn encode_producer(producer: &dyn ErasedProducer, limit: usize) -> Result<Vec<u8>, AdbcError> {
    let encoded = producer.encode()?;
    if encoded.len() > limit {
        return Err(invalid_data(
            "Result producer state exceeds configured limit",
        ));
    }
    Ok(encoded)
}

/// Decode, produce one batch, validate it, and re-encode the advanced state.
fn produce_from_state(
    decode: ProducerDecoder,
    state: &[u8],
    schema: &SchemaRef,
    max_batch_bytes: usize,
    state_limit: usize,
) -> Result<Option<(RecordBatch, Vec<u8>)>, AdbcError> {
    let mut producer = decode(state)?;
    let Some(batch) = producer.produce()? else {
        return Ok(None);
    };
    if batch.schema().as_ref() != schema.as_ref() {
        return Err(invalid_data("Result schema changed"));
    }
    if batch.get_array_memory_size() > max_batch_bytes {
        return Err(invalid_data("Result batch exceeds configured limit"));
    }
    let advanced = encode_producer(producer.as_ref(), state_limit)?;
    Ok(Some((batch, advanced)))
}

fn quota(kind: &str) -> AdbcError {
    AdbcError::with_message_and_status(format!("{kind} quota exceeded"), Status::InvalidState)
}

fn internal(message: impl Into<String>) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::Internal)
}

fn busy(message: impl Into<String>) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::InvalidState)
}

fn timeout(message: impl Into<String>) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::Timeout)
}

fn invalid_data(message: impl Into<String>) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::InvalidData)
}

fn not_found(kind: &str) -> AdbcError {
    AdbcError::with_message_and_status(format!("{kind} was not found"), Status::NotFound)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    use adbc_core::error::{Error, Result, Status};
    use adbc_core::options::{InfoCode, ObjectDepth, OptionValue};
    use adbc_core::{CancelHandle, PartitionedResult};
    use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
    use arrow_schema::{ArrowError, Schema};

    use super::{SessionLimits, SessionManager, TargetAuthorizer, TransportTargetAccess};
    use crate::backend::{Backend, BackendConnection, BackendStatement};
    use crate::config::TargetConfig;

    struct DummyBackend;
    struct DummyConnection;
    struct DummyStatement;
    struct DummyCancel;

    impl Backend for DummyBackend {
        fn open(
            &self,
            _target: &TargetConfig,
            _database_options: Vec<(String, OptionValue)>,
            _connection_options: Vec<(String, OptionValue)>,
        ) -> Result<Box<dyn BackendConnection>> {
            Ok(Box::new(DummyConnection))
        }
    }

    impl BackendConnection for DummyConnection {
        fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
            Arc::new(DummyCancel)
        }

        fn new_statement(&mut self) -> Result<Box<dyn BackendStatement>> {
            Ok(Box::new(DummyStatement))
        }

        fn set_option(&mut self, _key: &str, _value: OptionValue) -> Result<()> {
            unsupported()
        }

        fn get_option_string(&self, _key: &str) -> Result<String> {
            unsupported()
        }

        fn get_option_bytes(&self, _key: &str) -> Result<Vec<u8>> {
            unsupported()
        }

        fn get_option_int(&self, _key: &str) -> Result<i64> {
            unsupported()
        }

        fn get_option_double(&self, _key: &str) -> Result<f64> {
            unsupported()
        }

        fn get_info(
            &self,
            _codes: Option<HashSet<InfoCode>>,
        ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn get_objects(
            &self,
            _depth: ObjectDepth,
            _catalog: Option<&str>,
            _db_schema: Option<&str>,
            _table_name: Option<&str>,
            _table_type: Option<Vec<&str>>,
            _column_name: Option<&str>,
        ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn get_table_schema(
            &self,
            _catalog: Option<&str>,
            _db_schema: Option<&str>,
            _table_name: &str,
        ) -> Result<Schema> {
            unsupported()
        }

        fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn get_statistics(
            &self,
            _catalog: Option<&str>,
            _db_schema: Option<&str>,
            _table_name: Option<&str>,
            _approximate: bool,
        ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn commit(&mut self) -> Result<()> {
            Ok(())
        }

        fn rollback(&mut self) -> Result<()> {
            Ok(())
        }

        fn read_partition(
            &self,
            _partition: &[u8],
        ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }
    }

    impl BackendStatement for DummyStatement {
        fn set_option(&mut self, _key: &str, _value: OptionValue) -> Result<()> {
            unsupported()
        }

        fn get_option_string(&self, _key: &str) -> Result<String> {
            unsupported()
        }

        fn get_option_bytes(&self, _key: &str) -> Result<Vec<u8>> {
            unsupported()
        }

        fn get_option_int(&self, _key: &str) -> Result<i64> {
            unsupported()
        }

        fn get_option_double(&self, _key: &str) -> Result<f64> {
            unsupported()
        }

        fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
            unsupported()
        }

        fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
            unsupported()
        }

        fn set_sql_query(&mut self, _query: &str) -> Result<()> {
            unsupported()
        }

        fn set_substrait_plan(&mut self, _plan: &[u8]) -> Result<()> {
            unsupported()
        }

        fn prepare(&mut self) -> Result<()> {
            unsupported()
        }

        fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn execute_update(&mut self) -> Result<Option<i64>> {
            unsupported()
        }

        fn execute_schema(&mut self) -> Result<Schema> {
            unsupported()
        }

        fn execute_partitions(&mut self) -> Result<PartitionedResult> {
            unsupported()
        }

        fn get_parameter_schema(&self) -> Result<Schema> {
            unsupported()
        }

        fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
            Arc::new(DummyCancel)
        }
    }

    impl CancelHandle for DummyCancel {
        fn try_cancel(&self) -> Result<()> {
            Ok(())
        }
    }

    fn unsupported<T>() -> Result<T> {
        Err(Error::with_message_and_status(
            "not used by session tests",
            Status::NotImplemented,
        ))
    }

    fn target() -> TargetConfig {
        TargetConfig {
            driver: Some("dummy".to_string()),
            profile: None,
            entrypoint: None,
            database_options: Vec::new(),
            connection_options: Vec::new(),
            allow_client_database_options: false,
            allow_client_connection_options: false,
            allowed_client_database_options: Vec::new(),
            allowed_client_connection_options: Vec::new(),
            init_statements: Vec::new(),
        }
    }

    fn manager(
        limits: SessionLimits,
        authorizer: TargetAuthorizer,
        ttl: Duration,
    ) -> SessionManager {
        SessionManager::with_limits_and_authorizer(
            Arc::new(DummyBackend),
            HashMap::from([("sqlite".to_string(), target())]),
            ttl,
            true,
            limits,
            authorizer,
        )
    }

    fn open(manager: &SessionManager, principal: &str) -> String {
        manager
            .open(
                format!("bearer\0{principal}"),
                "sqlite",
                Vec::new(),
                Vec::new(),
            )
            .expect("open session")
    }

    fn empty_reader() -> Box<dyn RecordBatchReader + Send + 'static> {
        let schema = Arc::new(Schema::empty());
        Box::new(RecordBatchIterator::new(
            Vec::<std::result::Result<RecordBatch, ArrowError>>::new(),
            schema,
        ))
    }

    #[test]
    fn enforces_target_authorization_and_principal_isolation() {
        let authorizer = TargetAuthorizer::new(HashMap::from([(
            "alice".to_string(),
            vec!["sqlite".to_string()],
        )]));
        let manager = manager(
            SessionLimits::default(),
            authorizer,
            Duration::from_secs(60),
        );

        let id = open(&manager, "alice");
        let error = match manager.get(&id, "bearer\0bob") {
            Ok(_) => panic!("another principal must not acquire the session"),
            Err(error) => error,
        };
        assert_eq!(error.status, Status::Unauthorized);
        let error = manager
            .open("bearer\0bob".to_string(), "sqlite", Vec::new(), Vec::new())
            .expect_err("unlisted principal must be denied");
        assert_eq!(error.status, Status::Unauthorized);
    }

    #[test]
    fn enforces_session_statement_and_result_quotas() {
        let limits = SessionLimits {
            max_sessions: 2,
            max_sessions_per_principal: 1,
            max_statements_per_session: 1,
            max_results_per_session: 1,
        };
        let manager = manager(limits, TargetAuthorizer::default(), Duration::from_secs(60));
        let alice = open(&manager, "alice");
        assert!(
            manager
                .open(
                    "bearer\0alice".to_string(),
                    "sqlite",
                    Vec::new(),
                    Vec::new()
                )
                .is_err()
        );
        let _bob = open(&manager, "bob");
        assert!(
            manager
                .open(
                    "bearer\0charlie".to_string(),
                    "sqlite",
                    Vec::new(),
                    Vec::new()
                )
                .is_err()
        );

        let session = manager
            .get(&alice, "bearer\0alice")
            .expect("get own session");
        let statement = session.new_statement().expect("first statement");
        assert!(session.new_statement().is_err());
        session
            .close_statement(&statement)
            .expect("release statement quota");
        session
            .new_statement()
            .expect("statement quota was released");

        let (result, _) = session.insert_result(empty_reader()).expect("first result");
        assert!(session.insert_result(empty_reader()).is_err());
        session.close_result(&result).expect("release result quota");
        session
            .insert_result(empty_reader())
            .expect("result quota was released");
    }

    #[test]
    fn reaps_expired_sessions_and_closes_everything() {
        let manager = manager(
            SessionLimits::default(),
            TargetAuthorizer::default(),
            Duration::from_millis(1),
        );
        let id = open(&manager, "alice");
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(manager.reap_expired().expect("reap"), 1);
        assert_eq!(manager.session_count(), 0);
        let error = match manager.get(&id, "bearer\0alice") {
            Ok(_) => panic!("expired session must not be returned"),
            Err(error) => error,
        };
        assert_eq!(error.status, Status::NotFound);

        open(&manager, "alice");
        open(&manager, "bob");
        assert_eq!(manager.close_all().expect("close all"), 2);
        assert_eq!(manager.session_count(), 0);
    }

    #[test]
    fn validates_limit_relationships() {
        let invalid = SessionLimits {
            max_sessions: 1,
            max_sessions_per_principal: 2,
            max_statements_per_session: 1,
            max_results_per_session: 1,
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn transport_disconnect_only_revokes_its_own_sessions() {
        let manager = manager(
            SessionLimits::default(),
            TargetAuthorizer::default(),
            Duration::from_secs(3600),
        );
        let principal = "iroh\0alice";
        let first = manager.open_transport(principal.into()).unwrap();
        let second = manager.open_transport(principal.into()).unwrap();
        let first_session = manager
            .open_on_transport(principal.into(), "sqlite", vec![], vec![], Some(&first))
            .unwrap();
        let another = manager
            .open_on_transport(principal.into(), "sqlite", vec![], vec![], Some(&first))
            .unwrap();
        let surviving = manager
            .open_on_transport(principal.into(), "sqlite", vec![], vec![], Some(&second))
            .unwrap();
        let unbound = manager
            .open(principal.into(), "sqlite", vec![], vec![])
            .unwrap();
        assert!(
            manager
                .open_on_transport("iroh\0bob".into(), "sqlite", vec![], vec![], Some(&second))
                .is_err()
        );
        assert_eq!(manager.close_transport(&first).unwrap(), 2);
        assert_eq!(manager.close_transport(&first).unwrap(), 0);
        assert!(manager.get(&first_session, principal).is_err());
        assert!(manager.get(&another, principal).is_err());
        assert!(manager.get(&surviving, principal).is_ok());
        assert!(manager.get(&unbound, principal).is_ok());
        assert!(
            manager
                .open_on_transport(principal.into(), "sqlite", vec![], vec![], Some(&first))
                .is_err()
        );
        assert_eq!(manager.close_transport(&second).unwrap(), 1);
        assert_eq!(manager.session_count(), 1);
        manager.close_all().unwrap();
        assert!(manager.registry.lock().unwrap().transports.is_empty());
    }

    #[test]
    fn disconnect_during_open_cannot_leave_a_session_or_reservation() {
        struct DelayedBackend {
            started: mpsc::SyncSender<()>,
            finish: Mutex<mpsc::Receiver<()>>,
        }
        impl Backend for DelayedBackend {
            fn open(
                &self,
                target: &TargetConfig,
                database_options: Vec<(String, OptionValue)>,
                connection_options: Vec<(String, OptionValue)>,
            ) -> Result<Box<dyn BackendConnection>> {
                self.started.send(()).unwrap();
                self.finish.lock().unwrap().recv().unwrap();
                DummyBackend.open(target, database_options, connection_options)
            }
        }
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (finish_tx, finish_rx) = mpsc::sync_channel(1);
        let manager = Arc::new(SessionManager::with_limits_and_authorizer(
            Arc::new(DelayedBackend {
                started: started_tx,
                finish: Mutex::new(finish_rx),
            }),
            HashMap::from([("sqlite".into(), target())]),
            Duration::from_secs(3600),
            true,
            SessionLimits::default(),
            TargetAuthorizer::default(),
        ));
        let transport = manager.open_transport("iroh\0alice".into()).unwrap();
        let opener = {
            let manager = Arc::clone(&manager);
            let transport = transport.clone();
            thread::spawn(move || {
                manager.open_on_transport(
                    "iroh\0alice".into(),
                    "sqlite",
                    vec![],
                    vec![],
                    Some(&transport),
                )
            })
        };
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(manager.close_transport(&transport).unwrap(), 0);
        finish_tx.send(()).unwrap();
        assert_eq!(opener.join().unwrap().unwrap_err().status, Status::NotFound);
        let registry = manager.registry.lock().unwrap();
        assert!(registry.sessions.is_empty());
        assert!(registry.transports.is_empty());
        assert_eq!(registry.opening_total, 0);
        assert!(registry.opening_by_principal.is_empty());
    }

    #[test]
    fn shared_transport_grants_are_restricted_even_when_general_policy_allows_all() {
        for permissions in [
            HashMap::new(),
            HashMap::from([("operator".into(), vec!["private".into()])]),
            HashMap::from([("new-key".into(), vec!["private".into()])]),
        ] {
            let manager = SessionManager::with_limits_and_authorizer(
                Arc::new(DummyBackend),
                HashMap::from([("sqlite".into(), target()), ("private".into(), target())]),
                Duration::from_secs(3600),
                true,
                SessionLimits::default(),
                TargetAuthorizer::new(permissions),
            );
            let principal = "iroh-key\0new-key";
            let transport = manager
                .open_transport_with_access(
                    principal.into(),
                    Some(TransportTargetAccess {
                        public_targets: Arc::new(HashSet::from(["sqlite".into()])),
                        allow_principal_targets: false,
                    }),
                )
                .unwrap();
            manager
                .open_on_transport(principal.into(), "sqlite", vec![], vec![], Some(&transport))
                .unwrap();
            assert_eq!(
                manager
                    .open_on_transport(
                        principal.into(),
                        "private",
                        vec![],
                        vec![],
                        Some(&transport)
                    )
                    .unwrap_err()
                    .status,
                Status::Unauthorized
            );
            assert!(
                manager
                    .open_on_transport(
                        "bearer\0operator".into(),
                        "sqlite",
                        vec![],
                        vec![],
                        Some(&transport)
                    )
                    .is_err()
            );
            manager.close_transport(&transport).unwrap();
            assert!(
                manager
                    .open_on_transport(principal.into(), "sqlite", vec![], vec![], Some(&transport))
                    .is_err()
            );
        }
    }

    #[test]
    fn public_targets_do_not_grant_access_to_http_or_other_transports() {
        let manager = manager(
            SessionLimits::default(),
            TargetAuthorizer::new(HashMap::from([("operator".into(), vec!["sqlite".into()])])),
            Duration::from_secs(3600),
        );
        let transport = manager
            .open_transport_with_access(
                "iroh-key\0key".into(),
                Some(TransportTargetAccess {
                    public_targets: Arc::new(HashSet::from(["sqlite".into()])),
                    allow_principal_targets: false,
                }),
            )
            .unwrap();
        assert_eq!(
            manager
                .open("bearer\0guest".into(), "sqlite", vec![], vec![])
                .unwrap_err()
                .status,
            Status::Unauthorized
        );
        assert_eq!(
            manager
                .open("iroh-key\0key".into(), "sqlite", vec![], vec![])
                .unwrap_err()
                .status,
            Status::Unauthorized
        );
        assert!(
            manager
                .open_on_transport(
                    "iroh-key\0key".into(),
                    "sqlite",
                    vec![],
                    vec![],
                    Some(&transport)
                )
                .is_ok()
        );
    }
}
