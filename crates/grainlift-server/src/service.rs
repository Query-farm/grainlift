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

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::Error as AdbcError;
use adbc_core::options::{InfoCode, ObjectDepth, OptionValue};
use arrow_array::{BooleanArray, RecordBatch};
use grainlift_protocol as protocol;
use serde::{Deserialize, Serialize};
use vgi_rpc::StreamState;
use vgi_rpc::server::{MethodInfo, MethodType, RpcServer, StateDecoder};
use vgi_rpc::stream::{
    ExchangeState, OutputCollector, ProducerState, StreamResult, StreamStateKind,
};
use vgi_rpc::stream_codec::StreamStateCodec;
use vgi_rpc::{CallContext, Request, RpcError};

use crate::session::{BindMode, SessionManager};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BindCursor {
    session_id: String,
    upload_id: String,
    max_bytes: usize,
}

struct BindExchange {
    manager: Arc<SessionManager>,
    cursor: BindCursor,
}

impl ExchangeState for BindExchange {
    fn exchange(
        &mut self,
        input: &RecordBatch,
        out: &mut OutputCollector,
        ctx: &CallContext,
    ) -> vgi_rpc::Result<()> {
        let principal = self.manager.principal(&ctx.auth)?;
        let session = self
            .manager
            .get(&self.cursor.session_id, &principal)
            .map_err(adbc_rpc_error)?;
        let batch = match protocol::decode_bind_turn(
            input,
            self.cursor.max_bytes.min(protocol::MAX_CONTROL_BYTES),
        ) {
            Ok(batch) => batch,
            Err(error) => {
                session.cancel_bind_upload(&self.cursor.upload_id);
                return Err(protocol_rpc_error(error));
            }
        };
        if let Some(batch) = batch {
            if let Err(error) = session.push_bind_upload(&self.cursor.upload_id, batch) {
                session.cancel_bind_upload(&self.cursor.upload_id);
                return Err(adbc_rpc_error(error));
            }
            out.emit(bind_ack()?)?;
        } else {
            session
                .finish_bind_upload(&self.cursor.upload_id)
                .map_err(adbc_rpc_error)?;
            out.emit(bind_ack()?)?;
            out.finish();
        }
        Ok(())
    }

    fn on_cancel(&mut self, ctx: &CallContext) {
        if let Ok(principal) = self.manager.principal(&ctx.auth)
            && let Ok(session) = self.manager.get(&self.cursor.session_id, &principal)
        {
            session.cancel_bind_upload(&self.cursor.upload_id);
        }
    }

    fn encode_state(&self) -> vgi_rpc::Result<Vec<u8>> {
        serde_json::to_vec(&self.cursor)
            .map_err(|error| RpcError::runtime_error(format!("encode bind cursor: {error}")))
    }
}

/// Sealed into each `read_result` continuation token. Reader results keep
/// their cursor in the session; producer results carry their encoded state
/// here instead.
#[derive(Clone, Debug, Serialize, Deserialize, StreamState)]
struct ResultCursor {
    session_id: String,
    result_id: String,
    sequence: i64,
    producer: Option<Vec<u8>>,
}

struct ResultStream {
    manager: Arc<SessionManager>,
    cursor: ResultCursor,
}

impl ProducerState for ResultStream {
    fn produce(&mut self, out: &mut OutputCollector, ctx: &CallContext) -> vgi_rpc::Result<()> {
        let principal = self.manager.principal(&ctx.auth)?;
        let session = self
            .manager
            .get(&self.cursor.session_id, &principal)
            .map_err(adbc_rpc_error)?;
        let batch = match self.cursor.producer.take() {
            None => session
                .next_result(&self.cursor.result_id, self.cursor.sequence)
                .map_err(adbc_rpc_error)?,
            Some(state) => {
                let (batch, advanced) = session
                    .next_produced_result(
                        &self.cursor.result_id,
                        self.cursor.sequence,
                        state,
                        response_limit(ctx),
                    )
                    .map_err(adbc_rpc_error)?;
                self.cursor.producer = Some(advanced);
                batch
            }
        };
        match batch {
            Some(batch) => {
                out.emit(batch)?;
                self.cursor.sequence += 1;
            }
            None => out.finish(),
        }
        Ok(())
    }

    fn on_cancel(&mut self, ctx: &CallContext) {
        if let Ok(principal) = self.manager.principal(&ctx.auth)
            && let Ok(session) = self.manager.get(&self.cursor.session_id, &principal)
        {
            let _ = session.close_result(&self.cursor.result_id);
        }
    }

    fn encode_state(&self) -> vgi_rpc::Result<Vec<u8>> {
        self.cursor.encode()
    }
}

pub fn build_server(manager: Arc<SessionManager>, server_id: String) -> RpcServer {
    build_server_with_max_bind(manager, server_id, protocol::MAX_BIND_STREAM_BYTES)
}

pub fn build_server_with_max_bind(
    manager: Arc<SessionManager>,
    server_id: String,
    max_bind_bytes: usize,
) -> RpcServer {
    build_server_with_storage(manager, server_id, max_bind_bytes, None)
}

/// The server, storing result batches over the threshold in `external`
/// (VGI-RPC external locations) when given.
pub fn build_server_with_storage(
    manager: Arc<SessionManager>,
    server_id: String,
    max_bind_bytes: usize,
    external: Option<vgi_rpc::external::ExternalLocationConfig>,
) -> RpcServer {
    let hook = vgi_rpc::OtelHook::new(vgi_rpc::OtelConfig {
        service_name: "grainlift".to_string(),
        record_exceptions: false,
    });
    let mut server = RpcServer::builder()
        .server_id(server_id)
        .server_version(env!("CARGO_PKG_VERSION"))
        .protocol_name(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
        .with_hook(hook);
    if let Some(external) = external {
        server = server.with_external_location(external);
    }
    let mut server = server.build();

    register_open_connection(&mut server, manager.clone());
    register_session_operations(&mut server, manager.clone());
    register_connection_surface(&mut server, manager.clone());
    register_statement_operations(&mut server, manager.clone(), max_bind_bytes);
    register_result_operations(&mut server, manager);
    server
}

fn register_open_connection(server: &mut RpcServer, manager: Arc<SessionManager>) {
    server.register(
        MethodInfo::unary(
            protocol::method::OPEN_CONNECTION,
            protocol::typed_request_schema(),
            protocol::unary_response_schema(),
            move |request, ctx| {
                let args: protocol::OpenConnectionRequest = typed_request(request)?;
                let principal = manager.principal(&ctx.auth)?;
                let database_options = protocol::named_options_into_adbc(args.database_options)
                    .map_err(protocol_rpc_error)?;
                let connection_options = protocol::named_options_into_adbc(args.connection_options)
                    .map_err(protocol_rpc_error)?;
                let session_id = manager
                    .open_on_transport(
                        principal.clone(),
                        &args.target,
                        database_options,
                        connection_options,
                        crate::iroh_lifecycle::transport_id(ctx),
                    )
                    .map_err(adbc_rpc_error)?;
                Ok(Some(handle_response(
                    protocol::SessionResponse {
                        session_id: session_id.clone(),
                    },
                    response_limit(ctx),
                    || {
                        let _ = manager.close(&session_id, &principal);
                    },
                )?))
            },
        )
        .param_type("request", "OpenConnectionRequest")
        .doc("Open a server-side ADBC connection to an authorized target"),
    );
}

fn register_session_operations(server: &mut RpcServer, manager: Arc<SessionManager>) {
    type SessionOperation = fn(&crate::session::Session) -> Result<(), AdbcError>;
    let operations: [(&str, SessionOperation); 2] = [
        (protocol::method::COMMIT, crate::session::Session::commit),
        (
            protocol::method::ROLLBACK,
            crate::session::Session::rollback,
        ),
    ];
    for (name, operation) in operations {
        let manager = manager.clone();
        server.register(MethodInfo::unary(
            name,
            protocol::session_schema(),
            protocol::unary_response_schema(),
            move |request, ctx| {
                let (session, _) = get_session(&manager, request, ctx)?;
                operation(&session).map_err(adbc_rpc_error)?;
                Ok(Some(ok_batch()?))
            },
        ));
    }

    let cancel_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::CANCEL_CONNECTION,
        protocol::session_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&cancel_manager, request, ctx)?;
            session.cancel_connection().map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let close_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::CLOSE_CONNECTION,
        protocol::session_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            validate_protocol_version(request)?;
            let principal = close_manager.principal(&ctx.auth)?;
            close_manager
                .close(string(request, "session_id")?, &principal)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let statement_manager = manager;
    server.register(MethodInfo::unary(
        protocol::method::NEW_STATEMENT,
        protocol::session_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, session_id) = get_session(&statement_manager, request, ctx)?;
            let statement_id = session.new_statement().map_err(adbc_rpc_error)?;
            Ok(Some(handle_response(
                protocol::StatementResponse {
                    session_id,
                    statement_id: statement_id.clone(),
                },
                response_limit(ctx),
                || {
                    let _ = session.close_statement(&statement_id);
                },
            )?))
        },
    ));
}

fn register_connection_surface(server: &mut RpcServer, manager: Arc<SessionManager>) {
    let set_manager = manager.clone();
    server.register(
        MethodInfo::unary(
            protocol::method::SET_CONNECTION_OPTION,
            protocol::typed_request_schema(),
            protocol::unary_response_schema(),
            move |request, ctx| {
                let args: protocol::SetConnectionOptionRequest = typed_request(request)?;
                let session = session_by_id(&set_manager, &args.session_id, ctx)?;
                let value = args.value.into_adbc().map_err(protocol_rpc_error)?;
                session
                    .set_connection_option(args.key, value)
                    .map_err(adbc_rpc_error)?;
                Ok(Some(ok_batch()?))
            },
        )
        .param_type("request", "SetConnectionOptionRequest"),
    );

    let get_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_CONNECTION_OPTION,
        protocol::connection_option_key_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&get_manager, request, ctx)?;
            let key = string(request, "key")?.to_string();
            let value_type = string(request, "value_type")?.to_string();
            let value = session
                .with_connection(move |connection| match value_type.as_str() {
                    "string" => connection.get_option_string(&key).map(OptionValue::String),
                    "bytes" => connection.get_option_bytes(&key).map(OptionValue::Bytes),
                    "int" => connection.get_option_int(&key).map(OptionValue::Int),
                    "double" => connection.get_option_double(&key).map(OptionValue::Double),
                    _ => Err(AdbcError::with_message_and_status(
                        "unknown option value type",
                        adbc_core::error::Status::InvalidArguments,
                    )),
                })
                .map_err(adbc_rpc_error)?;
            Ok(Some(value_response(&value)?))
        },
    ));

    let info_manager = manager.clone();
    server.register(
        MethodInfo::unary(
            protocol::method::GET_INFO,
            protocol::typed_request_schema(),
            protocol::unary_response_schema(),
            move |request, ctx| {
                let args: protocol::GetInfoRequest = typed_request(request)?;
                let session = session_by_id(&info_manager, &args.session_id, ctx)?;
                let codes = args.codes.map(|values| {
                    values
                        .into_iter()
                        .map(|code| InfoCode::from(code as u32))
                        .collect::<HashSet<_>>()
                });
                let reader = session
                    .with_connection(move |connection| connection.get_info(codes))
                    .map_err(adbc_rpc_error)?;
                Ok(Some(insert_reader_response(&session, reader, ctx)?))
            },
        )
        .param_type("request", "GetInfoRequest"),
    );

    let objects_manager = manager.clone();
    server.register(
        MethodInfo::unary(
            protocol::method::GET_OBJECTS,
            protocol::typed_request_schema(),
            protocol::unary_response_schema(),
            move |request, ctx| {
                let args: protocol::GetObjectsRequest = typed_request(request)?;
                let session = session_by_id(&objects_manager, &args.session_id, ctx)?;
                let depth = ObjectDepth::try_from(args.depth as i32).map_err(adbc_rpc_error)?;
                let reader = session
                    .with_connection(move |connection| {
                        let table_type = args
                            .table_types
                            .as_ref()
                            .map(|values| values.iter().map(String::as_str).collect());
                        connection.get_objects(
                            depth,
                            args.catalog.as_deref(),
                            args.db_schema.as_deref(),
                            args.table_name.as_deref(),
                            table_type,
                            args.column_name.as_deref(),
                        )
                    })
                    .map_err(adbc_rpc_error)?;
                Ok(Some(insert_reader_response(&session, reader, ctx)?))
            },
        )
        .param_type("request", "GetObjectsRequest"),
    );

    let table_schema_manager = manager.clone();
    server.register(
        MethodInfo::unary(
            protocol::method::GET_TABLE_SCHEMA,
            protocol::typed_request_schema(),
            protocol::unary_response_schema(),
            move |request, ctx| {
                let args: protocol::GetTableSchemaRequest = typed_request(request)?;
                let session = session_by_id(&table_schema_manager, &args.session_id, ctx)?;
                let schema = session
                    .with_connection(move |connection| {
                        connection.get_table_schema(
                            args.catalog.as_deref(),
                            args.db_schema.as_deref(),
                            &args.table_name,
                        )
                    })
                    .map_err(adbc_rpc_error)?;
                Ok(Some(schema_response(&schema)?))
            },
        )
        .param_type("request", "GetTableSchemaRequest"),
    );

    let table_types_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_TABLE_TYPES,
        protocol::session_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&table_types_manager, request, ctx)?;
            let reader = session
                .with_connection(|connection| connection.get_table_types())
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader, ctx)?))
        },
    ));

    let statistic_names_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_STATISTIC_NAMES,
        protocol::session_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&statistic_names_manager, request, ctx)?;
            let reader = session
                .with_connection(|connection| connection.get_statistic_names())
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader, ctx)?))
        },
    ));

    let statistics_manager = manager.clone();
    server.register(
        MethodInfo::unary(
            protocol::method::GET_STATISTICS,
            protocol::typed_request_schema(),
            protocol::unary_response_schema(),
            move |request, ctx| {
                let args: protocol::GetStatisticsRequest = typed_request(request)?;
                let session = session_by_id(&statistics_manager, &args.session_id, ctx)?;
                let reader = session
                    .with_connection(move |connection| {
                        connection.get_statistics(
                            args.catalog.as_deref(),
                            args.db_schema.as_deref(),
                            args.table_name.as_deref(),
                            args.approximate,
                        )
                    })
                    .map_err(adbc_rpc_error)?;
                Ok(Some(insert_reader_response(&session, reader, ctx)?))
            },
        )
        .param_type("request", "GetStatisticsRequest"),
    );

    let partition_manager = manager;
    server.register(MethodInfo::unary(
        protocol::method::READ_PARTITION,
        protocol::connection_binary_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&partition_manager, request, ctx)?;
            let partition = partition_manager
                .open_partition(
                    &session,
                    binary(request, "payload")?,
                    protocol::MAX_CONTROL_BYTES,
                )
                .map_err(adbc_rpc_error)?;
            let reader = session
                .with_connection(move |connection| connection.read_partition(&partition))
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader, ctx)?))
        },
    ));
}

fn register_statement_operations(
    server: &mut RpcServer,
    manager: Arc<SessionManager>,
    max_bind_bytes: usize,
) {
    let set_option_manager = manager.clone();
    server.register(
        MethodInfo::unary(
            protocol::method::SET_STATEMENT_OPTION,
            protocol::typed_request_schema(),
            protocol::unary_response_schema(),
            move |request, ctx| {
                let args: protocol::SetStatementOptionRequest = typed_request(request)?;
                let session = session_by_id(&set_option_manager, &args.session_id, ctx)?;
                let value = args.value.into_adbc().map_err(protocol_rpc_error)?;
                session
                    .with_statement(&args.statement_id, move |statement| {
                        statement.set_option(&args.key, value)
                    })
                    .map_err(adbc_rpc_error)?;
                Ok(Some(ok_batch()?))
            },
        )
        .param_type("request", "SetStatementOptionRequest"),
    );

    let get_option_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_STATEMENT_OPTION,
        protocol::statement_option_key_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&get_option_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?;
            let key = string(request, "key")?.to_string();
            let value_type = string(request, "value_type")?.to_string();
            let value = session
                .with_statement(statement_id, move |statement| match value_type.as_str() {
                    "string" => statement.get_option_string(&key).map(OptionValue::String),
                    "bytes" => statement.get_option_bytes(&key).map(OptionValue::Bytes),
                    "int" => statement.get_option_int(&key).map(OptionValue::Int),
                    "double" => statement.get_option_double(&key).map(OptionValue::Double),
                    _ => Err(AdbcError::with_message_and_status(
                        "unknown option value type",
                        adbc_core::error::Status::InvalidArguments,
                    )),
                })
                .map_err(adbc_rpc_error)?;
            Ok(Some(value_response(&value)?))
        },
    ));

    let set_sql_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::SET_SQL_QUERY,
        protocol::set_sql_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&set_sql_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?;
            let sql = string(request, "sql")?.to_string();
            session
                .with_statement(statement_id, move |statement| statement.set_sql_query(&sql))
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let substrait_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::SET_SUBSTRAIT_PLAN,
        protocol::statement_binary_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&substrait_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?;
            let plan = binary(request, "payload")?.to_vec();
            session
                .with_statement(statement_id, move |statement| {
                    statement.set_substrait_plan(&plan)
                })
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let prepare_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::PREPARE,
        protocol::statement_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&prepare_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?;
            session
                .with_statement(statement_id, |statement| statement.prepare())
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    register_bind_exchange(
        server,
        manager.clone(),
        protocol::method::BIND,
        BindMode::Batch,
        max_bind_bytes,
    );
    register_bind_exchange(
        server,
        manager.clone(),
        protocol::method::BIND_STREAM,
        BindMode::Stream,
        max_bind_bytes,
    );

    let cancel_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::CANCEL_STATEMENT,
        protocol::statement_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&cancel_manager, request, ctx)?;
            session
                .cancel_statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let execute_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::EXECUTE,
        protocol::statement_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&execute_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?.to_string();
            session
                .invalidate_statement_results(&statement_id)
                .map_err(adbc_rpc_error)?;
            let result = session
                .with_statement(&statement_id, |statement| statement.execute_result())
                .map_err(adbc_rpc_error)?;
            let (result_id, schema) = session
                .insert_statement_query_result(&statement_id, result)
                .map_err(adbc_rpc_error)?;
            Ok(Some(result_response(
                &session,
                &result_id,
                schema.as_ref(),
                response_limit(ctx),
            )?))
        },
    ));

    let update_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::EXECUTE_UPDATE,
        protocol::statement_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&update_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?.to_string();
            session
                .invalidate_statement_results(&statement_id)
                .map_err(adbc_rpc_error)?;
            let affected = session
                .with_statement(&statement_id, |statement| statement.execute_update())
                .map_err(adbc_rpc_error)?;
            Ok(Some(typed_response(protocol::UpdateResponse {
                rows_affected: affected,
            })?))
        },
    ));

    let schema_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::EXECUTE_SCHEMA,
        protocol::statement_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&schema_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?.to_string();
            session
                .invalidate_statement_results(&statement_id)
                .map_err(adbc_rpc_error)?;
            let schema = session
                .with_statement(&statement_id, |statement| statement.execute_schema())
                .map_err(adbc_rpc_error)?;
            Ok(Some(schema_response(&schema)?))
        },
    ));

    let parameter_schema_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_PARAMETER_SCHEMA,
        protocol::statement_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&parameter_schema_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?;
            let schema = session
                .with_statement(statement_id, |statement| statement.get_parameter_schema())
                .map_err(adbc_rpc_error)?;
            Ok(Some(schema_response(&schema)?))
        },
    ));

    let partitions_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::EXECUTE_PARTITIONS,
        protocol::statement_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&partitions_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?.to_string();
            session
                .invalidate_statement_results(&statement_id)
                .map_err(adbc_rpc_error)?;
            let result = session
                .with_statement(&statement_id, |statement| statement.execute_partitions())
                .map_err(adbc_rpc_error)?;
            let schema = protocol::encode_schema(&result.schema).map_err(protocol_rpc_error)?;
            let limit = response_limit(ctx);
            let mut remaining = limit
                .checked_sub(schema.len())
                .ok_or_else(|| RpcError::value_error("partition response exceeds limit"))?;
            let mut partitions = Vec::new();
            for descriptor in result.partitions {
                let token = partitions_manager
                    .seal_partition(&session, descriptor, remaining)
                    .map_err(adbc_rpc_error)?;
                remaining = remaining
                    .checked_sub(token.len() + std::mem::size_of::<protocol::Bytes>())
                    .ok_or_else(|| RpcError::value_error("partition response exceeds limit"))?;
                partitions.push(protocol::Bytes(token));
            }
            Ok(Some(
                bounded_response(
                    protocol::PartitionsResponse {
                        rows_affected: result.rows_affected,
                        schema_ipc: protocol::Bytes(schema),
                        partitions,
                    },
                    limit,
                )
                .map_err(protocol_rpc_error)?,
            ))
        },
    ));

    let close_manager = manager;
    server.register(MethodInfo::unary(
        protocol::method::CLOSE_STATEMENT,
        protocol::statement_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&close_manager, request, ctx)?;
            session
                .close_statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));
}

fn register_result_operations(server: &mut RpcServer, manager: Arc<SessionManager>) {
    let close_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::CLOSE_RESULT,
        protocol::result_schema(),
        protocol::unary_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&close_manager, request, ctx)?;
            session
                .close_result(string(request, "result_id")?)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let handler_manager = manager.clone();
    let decoder_manager = manager;
    let decoder: StateDecoder = Arc::new(move |bytes: &[u8]| {
        let cursor = ResultCursor::decode(bytes)
            .map_err(|error| RpcError::protocol_error(format!("decode result cursor: {error}")))?;
        Ok(StreamStateKind::Producer(Box::new(ResultStream {
            manager: decoder_manager.clone(),
            cursor,
        })))
    });
    server.register(
        MethodInfo::stream(
            protocol::method::READ_RESULT,
            MethodType::Producer,
            protocol::read_result_schema(),
            move |request, ctx| {
                let (session, session_id) = get_session(&handler_manager, request, ctx)?;
                let result_id = string(request, "result_id")?.to_string();
                let sequence = protocol::int64_value(&request.batch, "sequence")
                    .map_err(protocol_rpc_error)?;
                let (schema, producer) = session
                    .open_result_stream(&result_id, sequence)
                    .map_err(adbc_rpc_error)?;
                Ok(StreamResult::producer(
                    schema,
                    Box::new(ResultStream {
                        manager: handler_manager.clone(),
                        cursor: ResultCursor {
                            session_id,
                            result_id,
                            sequence,
                            producer,
                        },
                    }),
                ))
            },
        )
        .with_state_decoder(decoder),
    );
}

fn register_bind_exchange(
    server: &mut RpcServer,
    manager: Arc<SessionManager>,
    method: &'static str,
    mode: BindMode,
    max_bind_bytes: usize,
) {
    let handler_manager = manager.clone();
    let decoder_manager = manager;
    let decoder: StateDecoder = Arc::new(move |bytes: &[u8]| {
        let cursor: BindCursor = serde_json::from_slice(bytes)
            .map_err(|error| RpcError::protocol_error(format!("decode bind cursor: {error}")))?;
        Ok(StreamStateKind::Exchange(Box::new(BindExchange {
            manager: decoder_manager.clone(),
            cursor,
        })))
    });
    server.register(
        MethodInfo::stream(
            method,
            MethodType::Exchange,
            protocol::bind_init_schema(),
            move |request, ctx| {
                let (session, session_id) = get_session(&handler_manager, request, ctx)?;
                let statement_id = string(request, "statement_id")?;
                let schema = protocol::decode_schema(binary(request, "schema_ipc")?)
                    .map_err(protocol_rpc_error)?;
                let upload_id = session
                    .start_bind_upload(statement_id, mode, Arc::new(schema.clone()), max_bind_bytes)
                    .map_err(adbc_rpc_error)?;
                Ok(StreamResult::exchange(
                    protocol::empty_response_schema(),
                    protocol::bind_turn_schema(),
                    Box::new(BindExchange {
                        manager: handler_manager.clone(),
                        cursor: BindCursor {
                            session_id,
                            upload_id,
                            max_bytes: max_bind_bytes,
                        },
                    }),
                ))
            },
        )
        .with_state_decoder(decoder),
    );
}

fn insert_reader_response(
    session: &crate::session::Session,
    reader: Box<dyn arrow_array::RecordBatchReader + Send + 'static>,
    ctx: &CallContext,
) -> vgi_rpc::Result<RecordBatch> {
    let (result_id, schema) = session.insert_result(reader).map_err(adbc_rpc_error)?;
    result_response(session, &result_id, schema.as_ref(), response_limit(ctx))
}

fn result_response(
    session: &crate::session::Session,
    result_id: &str,
    schema: &arrow_schema::Schema,
    limit: usize,
) -> vgi_rpc::Result<RecordBatch> {
    let response = protocol::encode_schema(schema)
        .map(|schema_ipc| protocol::ExecuteResponse {
            result_id: result_id.into(),
            rows_affected: None,
            schema_ipc: protocol::Bytes(schema_ipc),
        })
        .and_then(|value| bounded_response(value, limit));
    if response.is_err() {
        let _ = session.close_result(result_id);
    }
    response.map_err(protocol_rpc_error)
}

fn response_limit(ctx: &CallContext) -> usize {
    ctx.response_limit_bytes
        .unwrap_or(protocol::MAX_CONTROL_BYTES)
        .min(protocol::MAX_CONTROL_BYTES)
}

fn handle_response<T: protocol::ResponseRecord>(
    value: T,
    limit: usize,
    cleanup: impl FnOnce(),
) -> vgi_rpc::Result<RecordBatch> {
    let response = bounded_response(value, limit);
    if response.is_err() {
        cleanup();
    }
    response.map_err(protocol_rpc_error)
}

fn bounded_response<T: protocol::ResponseRecord>(
    value: T,
    limit: usize,
) -> Result<RecordBatch, protocol::ProtocolError> {
    let response = protocol::encode_response(value, limit)?;
    // Check the complete outer IPC envelope as well as the nested record.
    // The transport still owns its metadata and final response-size enforcement.
    protocol::encode_batch_ipc(&response, limit)?;
    Ok(response)
}

fn schema_response(schema: &arrow_schema::Schema) -> vgi_rpc::Result<RecordBatch> {
    let bytes = protocol::encode_schema(schema).map_err(protocol_rpc_error)?;
    typed_response(protocol::SchemaResponse {
        schema_ipc: protocol::Bytes(bytes),
    })
}

fn value_response(value: &OptionValue) -> vgi_rpc::Result<RecordBatch> {
    let value = protocol::WireOptionValue::from(value);
    value.clone().into_adbc().map_err(protocol_rpc_error)?;
    typed_response(protocol::ValueResponse { value })
}

fn typed_request<T: protocol::RequestRecord>(request: &Request) -> vgi_rpc::Result<T> {
    validate_protocol_version(request)?;
    protocol::decode_request(&request.batch, protocol::MAX_CONTROL_BYTES)
        .map_err(protocol_rpc_error)
}

fn session_by_id(
    manager: &SessionManager,
    session_id: &str,
    ctx: &CallContext,
) -> vgi_rpc::Result<Arc<crate::session::Session>> {
    let principal = manager.principal(&ctx.auth)?;
    manager.get(session_id, &principal).map_err(adbc_rpc_error)
}

fn get_session(
    manager: &SessionManager,
    request: &Request,
    ctx: &CallContext,
) -> vgi_rpc::Result<(Arc<crate::session::Session>, String)> {
    validate_protocol_version(request)?;
    let principal = manager.principal(&ctx.auth)?;
    let session_id = string(request, "session_id")?.to_string();
    let session = manager
        .get(&session_id, &principal)
        .map_err(adbc_rpc_error)?;
    Ok((session, session_id))
}

fn validate_protocol_version(request: &Request) -> vgi_rpc::Result<()> {
    let compatible = request
        .metadata
        .get("vgi_rpc.protocol_version")
        .and_then(|version| version.strip_prefix("0.4."))
        .is_some_and(|patch| !patch.is_empty() && patch.bytes().all(|byte| byte.is_ascii_digit()));
    if compatible {
        Ok(())
    } else {
        Err(RpcError::version_error(
            "Grainlift requires an explicit 0.4.x protocol version",
        ))
    }
}

fn string<'a>(request: &'a Request, name: &str) -> vgi_rpc::Result<&'a str> {
    let value = protocol::string_value(&request.batch, name).map_err(protocol_rpc_error)?;
    if matches!(name, "session_id" | "statement_id" | "result_id" | "key") {
        protocol::validate_handle(value).map_err(protocol_rpc_error)?;
    } else {
        protocol::validate_text(value).map_err(protocol_rpc_error)?;
    }
    Ok(value)
}

fn binary<'a>(request: &'a Request, name: &str) -> vgi_rpc::Result<&'a [u8]> {
    protocol::binary_value(&request.batch, name).map_err(protocol_rpc_error)
}

fn ok_batch() -> vgi_rpc::Result<RecordBatch> {
    typed_response(protocol::OkResponse { ok: true })
}

fn typed_response<T: protocol::ResponseRecord>(value: T) -> vgi_rpc::Result<RecordBatch> {
    protocol::encode_response(value, protocol::MAX_CONTROL_BYTES).map_err(protocol_rpc_error)
}

fn bind_ack() -> vgi_rpc::Result<RecordBatch> {
    RecordBatch::try_new(
        protocol::empty_response_schema(),
        vec![Arc::new(BooleanArray::from(vec![true]))],
    )
    .map_err(arrow_rpc_error)
}

fn adbc_rpc_error(error: AdbcError) -> RpcError {
    let wire = protocol::WireAdbcError::from(&error);
    let encoded = serde_json::to_string(&wire).unwrap_or_else(|_| error.message.clone());
    RpcError::new("AdbcError", encoded)
        .with_error_kind(format!("adbc.{}", protocol::status_name(error.status)))
}

fn protocol_rpc_error(error: protocol::ProtocolError) -> RpcError {
    RpcError::value_error(error.to_string())
}

fn arrow_rpc_error(error: arrow_schema::ArrowError) -> RpcError {
    RpcError::new("ArrowError", error.to_string())
}

#[cfg(test)]
mod response_tests {
    use super::*;
    use std::cell::Cell;

    fn assert_cleanup_boundaries<T: protocol::ResponseRecord + Clone>(value: T) {
        let response = bounded_response(value.clone(), protocol::MAX_CONTROL_BYTES).unwrap();
        let size = protocol::encode_batch_ipc(&response, protocol::MAX_CONTROL_BYTES)
            .unwrap()
            .len();
        let cleaned = Cell::new(0);
        assert!(
            handle_response(value.clone(), size - 1, || cleaned.set(cleaned.get() + 1)).is_err()
        );
        assert_eq!(cleaned.get(), 1);
        for limit in [size, size + 1] {
            assert!(
                handle_response(value.clone(), limit, || cleaned.set(cleaned.get() + 1)).is_ok()
            );
        }
        assert_eq!(cleaned.get(), 1);
    }

    #[test]
    fn session_and_statement_encoding_failures_reclaim_only_failed_handles() {
        assert_cleanup_boundaries(protocol::SessionResponse {
            session_id: "session".into(),
        });
        assert_cleanup_boundaries(protocol::StatementResponse {
            session_id: "session".into(),
            statement_id: "statement".into(),
        });
    }
}
