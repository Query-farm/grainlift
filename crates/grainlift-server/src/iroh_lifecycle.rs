// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Connection-scoped cleanup for authenticated Iroh sessions.

use std::sync::Arc;

use vgi_rpc::{CallContext, ConnectionContext, RpcError};
use vgi_rpc_iroh::IrohConnectionLifecycle;

use crate::session::SessionManager;

// This claim is minted by the server lifecycle hook, never request metadata.
const CONNECTION_CLAIM: &str = "grainlift.internal.iroh_connection";

pub struct IrohSessionLifecycle {
    manager: Arc<SessionManager>,
}

impl IrohSessionLifecycle {
    pub fn new(manager: Arc<SessionManager>) -> Self {
        Self { manager }
    }
}

impl IrohConnectionLifecycle for IrohSessionLifecycle {
    fn opened(&self, context: &mut ConnectionContext) -> vgi_rpc::Result<()> {
        let principal = self.manager.principal(&context.auth)?;
        let id = self
            .manager
            .open_transport(principal)
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
