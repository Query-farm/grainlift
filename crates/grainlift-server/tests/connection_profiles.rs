// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_grainlift::{
    GrainliftDriver, OPTION_BEARER_TOKEN, OPTION_GRAINLIFT_URI, OPTION_TARGET,
};
use arrow_array::Int64Array;
use grainlift_server::backend::DriverManagerBackend;
use grainlift_server::config::Config;
use grainlift_server::hosting::require_credentials;
use grainlift_server::service::build_server;
use grainlift_server::session::SessionManager;
use vgi_rpc::AuthContext;
use vgi_rpc::auth::bearer::bearer_authenticate_static;
use vgi_rpc::http::HttpState;

// This exercises an installed native driver as well as the normal HTTP client.
// Run with: dbc install sqlite --level user
// cargo test -p grainlift-server --test connection_profiles -- --ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the SQLite ADBC driver installed with dbc"]
async fn profile_targets_preserve_options_authorization_and_cleanup_over_http() {
    let directory = tempfile::tempdir().unwrap();
    let profile = directory.path().join("reporting.toml");
    let valid_profile = "profile_version = 1\ndriver = 'sqlite'\n[Options]\nuri = ':memory:'\n";
    std::fs::write(&profile, valid_profile).unwrap();
    let config = Config::from_toml(&format!(
        r#"
[server]
max_sessions = 1
max_sessions_per_principal = 1
[auth.static_bearer_tokens]
reader-token = "reader"
outsider-token = "outsider"
[auth.target_permissions]
reader = ["analytics"]
[targets.analytics]
profile = {profile:?}
allow_client_database_options = true
allow_client_connection_options = true
"#,
        profile = profile.to_str().unwrap()
    ))
    .unwrap();
    let manager = Arc::new(SessionManager::with_limits_and_authorizer(
        Arc::new(DriverManagerBackend),
        config.targets.clone(),
        Duration::from_secs(60),
        true,
        config.server.session_limits(),
        config.target_authorizer(),
    ));
    let auth = bearer_authenticate_static(HashMap::from([
        (
            "reader-token".into(),
            AuthContext::for_principal("test", "reader"),
        ),
        (
            "outsider-token".into(),
            AuthContext::for_principal("test", "outsider"),
        ),
    ]));
    let state = HttpState::builder()
        .server(Arc::new(build_server(
            manager.clone(),
            "profile-test".into(),
        )))
        .authenticate(require_credentials(auth))
        .build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, vgi_rpc::http::build_router(state))
            .await
            .unwrap();
    });

    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let database = |token: &str, extra: Vec<(OptionDatabase, OptionValue)>| {
            GrainliftDriver.new_database_with_opts(
                [
                    (
                        OptionDatabase::Other(OPTION_GRAINLIFT_URI.into()),
                        endpoint.clone().into(),
                    ),
                    (
                        OptionDatabase::Other(OPTION_TARGET.into()),
                        "analytics".into(),
                    ),
                    (
                        OptionDatabase::Other(OPTION_BEARER_TOKEN.into()),
                        token.into(),
                    ),
                ]
                .into_iter()
                .chain(extra),
            )
        };
        {
            let database = database("reader-token", vec![])?;
            let mut connection = database.new_connection()?;
            connection.set_option(OptionConnection::AutoCommit, false.into())?;
            let mut statement = connection.new_statement()?;
            statement.set_sql_query("SELECT 42 AS answer")?;
            let batch = statement.execute()?.next().unwrap()?;
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                42
            );
            drop(statement);
            connection.commit()?;
            let error = connection
                .set_option(
                    OptionConnection::Other("uri".into()),
                    "credential-canary".into(),
                )
                .unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(error.message.contains("controlled by the proxy server"));
            assert!(!error.message.contains("credential-canary"));
        }

        for at_connection_level in [false, true] {
            let database = database(
                "reader-token",
                if at_connection_level {
                    vec![]
                } else {
                    vec![(OptionDatabase::Uri, "credential-canary".into())]
                },
            )?;
            let result = if at_connection_level {
                database.new_connection_with_opts([(
                    OptionConnection::Other("uri".into()),
                    "credential-canary".into(),
                )])
            } else {
                database.new_connection()
            };
            let error = result.err().expect("profile URI override must fail");
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(error.message.contains("controlled by the proxy server"));
            assert!(!error.message.contains("credential-canary"));
        }

        // Authentication and target authorization still precede profile access.
        std::fs::write(&profile, "password = 'profile-secret-canary").unwrap();
        let error = database("outsider-token", vec![])?
            .new_connection()
            .err()
            .unwrap();
        assert_eq!(error.status, Status::Unauthorized);
        let error = database("reader-token", vec![])?
            .new_connection()
            .err()
            .unwrap();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(!error.message.contains("profile-secret-canary"));
        assert!(!error.message.contains(profile.to_str().unwrap()));

        // A failed profile open releases its quota; the next open rereads it.
        std::fs::write(&profile, valid_profile).unwrap();
        drop(database("reader-token", vec![])?.new_connection()?);
        Ok(())
    })
    .await;
    task.abort();
    let _ = task.await;
    result.unwrap().unwrap();
    let counts = manager.resource_counts().unwrap();
    assert_eq!(counts.sessions, 0);
    assert_eq!(counts.opening_sessions, 0);
}
