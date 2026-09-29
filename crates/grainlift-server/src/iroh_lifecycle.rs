// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Connection-scoped cleanup for authenticated Iroh sessions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use vgi_rpc::{AuthContext, CallContext, ConnectionContext, PeerAuthenticationPolicy, RpcError};
use vgi_rpc_iroh::IrohConnectionLifecycle;

use crate::session::{SessionManager, TransportTargetAccess};

// This claim is minted by the server lifecycle hook, never request metadata.
const CONNECTION_CLAIM: &str = "grainlift.internal.iroh_connection";

pub struct IrohSessionLifecycle {
    manager: Arc<SessionManager>,
    principals: Arc<HashMap<String, String>>,
    public_targets: Arc<HashSet<String>>,
}

impl IrohSessionLifecycle {
    pub fn new(manager: Arc<SessionManager>) -> Self {
        Self {
            manager,
            principals: Arc::new(HashMap::new()),
            public_targets: Arc::new(HashSet::new()),
        }
    }

    /// Configure named peers and explicitly shared targets from server policy.
    pub fn with_access_policy(
        mut self,
        principals: HashMap<String, String>,
        public_targets: Vec<String>,
    ) -> Self {
        self.principals = Arc::new(principals);
        self.public_targets = Arc::new(public_targets.into_iter().collect());
        self
    }

    /// Authenticate only transport-verified endpoint IDs. Public peers retain
    /// a separate namespace from administrator-assigned principal aliases.
    pub fn authentication_policy(&self, require_authentication: bool) -> PeerAuthenticationPolicy {
        authentication_policy(
            Arc::clone(&self.principals),
            !self.public_targets.is_empty(),
            require_authentication,
        )
    }
}

fn authentication_policy(
    principals: Arc<HashMap<String, String>>,
    public_access: bool,
    require_authentication: bool,
) -> PeerAuthenticationPolicy {
    Arc::new(move |evidence, existing| {
        let identity = evidence.unique_verified_subject("iroh")?;
        let endpoint = identity
            .subject_key()
            .ok_or_else(|| RpcError::permission_error("Iroh endpoint identity is missing"))?;
        let mut auth = if let Some(principal) = principals.get(endpoint) {
            AuthContext::for_principal("iroh", principal)
        } else if public_access {
            AuthContext::for_principal("iroh-key", endpoint)
        } else if require_authentication || !principals.is_empty() {
            return Err(RpcError::permission_error(
                "Iroh endpoint is not authorized",
            ));
        } else {
            return Ok(existing.clone());
        };
        auth.claims.insert("subject".into(), endpoint.into());
        Ok(auth)
    })
}

impl IrohConnectionLifecycle for IrohSessionLifecycle {
    fn opened(&self, context: &mut ConnectionContext) -> vgi_rpc::Result<()> {
        let principal = self.manager.principal(&context.auth)?;
        let access = if self.public_targets.is_empty() {
            None
        } else {
            let identity = context.peer_evidence.unique_verified_subject("iroh")?;
            let endpoint = identity
                .subject_key()
                .ok_or_else(|| RpcError::permission_error("Iroh endpoint identity is missing"))?;
            Some(TransportTargetAccess {
                public_targets: Arc::clone(&self.public_targets),
                allow_principal_targets: self.principals.contains_key(endpoint),
            })
        };
        let id = self
            .manager
            .open_transport_with_access(principal, access)
            .map_err(|_| RpcError::runtime_error("could not register transport connection"))?;
        context.auth.claims.insert(CONNECTION_CLAIM.into(), id);
        Ok(())
    }

    fn closed(&self, context: &ConnectionContext) {
        if let Some(id) = context.auth.claims.get(CONNECTION_CLAIM)
            && self.manager.close_transport(id).is_err()
        {
            tracing::error!("could not revoke disconnected Iroh sessions");
        }
    }
}

pub(crate) fn transport_id(ctx: &CallContext) -> Option<&str> {
    // HTTP credentials may contain arbitrary claims. Only verified direct
    // Iroh evidence can activate connection ownership.
    ctx.peer_evidence.unique_verified_subject("iroh").ok()?;
    ctx.auth.claims.get(CONNECTION_CLAIM).map(String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vgi_rpc::auth::identity::{
        IdentityAssurance, PeerEvidenceSet, PeerIdentity, PeerIdentityResult, SubjectKind,
        SubjectStability,
    };

    fn evidence(key: &str, verified: bool) -> PeerEvidenceSet {
        PeerEvidenceSet::from_results([PeerIdentityResult::available(
            PeerIdentity::new(
                "iroh",
                "iroh_quic_handshake",
                IdentityAssurance::CryptographicPeer,
                "test",
                "iroh",
            )
            .unwrap()
            .with_subject(
                SubjectKind::Endpoint,
                key,
                SubjectStability::Stable,
                verified,
            )
            .unwrap(),
        )])
        .unwrap()
    }

    // Policy construction needs no backend: exercise the same factory through
    // a helper to keep authentication independent of database initialization.
    #[test]
    fn policy_preserves_allowlist_and_uses_verified_key_principals() {
        let policy = authentication_policy(
            Arc::new(HashMap::from([("known".into(), "alice".into())])),
            false,
            true,
        );
        let anonymous = AuthContext::anonymous();
        assert_eq!(
            policy(&evidence("known", true), &anonymous)
                .unwrap()
                .principal,
            "alice"
        );
        assert!(policy(&evidence("new", true), &anonymous).is_err());
        let public = authentication_policy(
            Arc::new(HashMap::from([("known".into(), "new".into())])),
            true,
            true,
        );
        let first = public(&evidence("new", true), &anonymous).unwrap();
        let second = public(&evidence("another", true), &anonymous).unwrap();
        let mapped = public(&evidence("known", true), &anonymous).unwrap();
        assert!(first.authenticated);
        assert_eq!(first.principal, "new");
        assert_eq!(first.domain, "iroh-key");
        assert_ne!(first.principal, second.principal);
        assert_ne!(first.domain, mapped.domain);
        assert!(public(&evidence("new", false), &anonymous).is_err());
        assert!(public(&PeerEvidenceSet::from_results([]).unwrap(), &anonymous).is_err());
    }
}
