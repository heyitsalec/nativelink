// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::pin::Pin;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::SystemTime;

use futures::stream::unfold;
use futures::{Stream, StreamExt};
use nativelink_config::schedulers::GrpcSpec;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_server::{
    Capabilities, CapabilitiesServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::{
    Execution, ExecutionServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ExecuteRequest, ExecutionCapabilities, GetCapabilitiesRequest, ServerCapabilities,
    WaitExecutionRequest, digest_function,
};
use nativelink_proto::google::longrunning::Operation;
use nativelink_scheduler::grpc_scheduler::GrpcScheduler;
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::action_messages::{
    ActionResult, ActionStage, ActionState, OperationId, WorkerId,
};
use nativelink_util::background_spawn;
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{ClientStateManager, OperationFilter};
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};
use utils::scheduler_utils::{INSTANCE_NAME, make_base_action_info};

mod utils {
    pub(crate) mod scheduler_utils;
}

type OperationStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send + 'static>>;

/// Queue of scripted response streams, popped by successive mock RPC calls.
type ScriptedStreams = Arc<Mutex<VecDeque<mpsc::UnboundedReceiver<Result<Operation, Status>>>>>;

/// Turns a scripted channel into the response stream of a mock RPC, so tests
/// can feed `Operation` messages one by one and close the stream by dropping
/// the sender.
fn receiver_to_stream(rx: mpsc::UnboundedReceiver<Result<Operation, Status>>) -> OperationStream {
    Box::pin(unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

/// An in-process mock of the upstream Execution/Capabilities services that
/// `GrpcScheduler` proxies to. Each `execute`/`wait_execution` call pops a
/// pre-registered scripted stream; requests are recorded for assertions.
#[derive(Clone, Default)]
struct MockUpstreamServer {
    execute_requests: Arc<Mutex<Vec<ExecuteRequest>>>,
    wait_execution_requests: Arc<Mutex<Vec<WaitExecutionRequest>>>,
    capabilities_requests: Arc<Mutex<Vec<GetCapabilitiesRequest>>>,
    supported_node_properties: Arc<Mutex<Vec<String>>>,
    pending_execute_streams: ScriptedStreams,
    pending_wait_execution_streams: ScriptedStreams,
}

impl MockUpstreamServer {
    /// Registers the stream handed out to the next `execute` call and
    /// returns the sender that scripts it.
    fn script_execute_stream(&self) -> mpsc::UnboundedSender<Result<Operation, Status>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending_execute_streams.lock().push_back(rx);
        tx
    }

    /// Registers the stream handed out to the next `wait_execution` call and
    /// returns the sender that scripts it.
    fn script_wait_execution_stream(&self) -> mpsc::UnboundedSender<Result<Operation, Status>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending_wait_execution_streams.lock().push_back(rx);
        tx
    }
}

#[tonic::async_trait]
impl Execution for MockUpstreamServer {
    type ExecuteStream = OperationStream;

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        self.execute_requests.lock().push(request.into_inner());
        let rx = self
            .pending_execute_streams
            .lock()
            .pop_front()
            .ok_or_else(|| Status::internal("No scripted execute stream in mock upstream"))?;
        Ok(Response::new(receiver_to_stream(rx)))
    }

    type WaitExecutionStream = OperationStream;

    async fn wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        self.wait_execution_requests
            .lock()
            .push(request.into_inner());
        let rx = self
            .pending_wait_execution_streams
            .lock()
            .pop_front()
            .ok_or_else(|| {
                Status::internal("No scripted wait_execution stream in mock upstream")
            })?;
        Ok(Response::new(receiver_to_stream(rx)))
    }
}

#[tonic::async_trait]
impl Capabilities for MockUpstreamServer {
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        self.capabilities_requests.lock().push(request.into_inner());
        Ok(Response::new(ServerCapabilities {
            execution_capabilities: Some(ExecutionCapabilities {
                supported_node_properties: self.supported_node_properties.lock().clone(),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}

/// Binds the mock upstream on an ephemeral local port and points a
/// `GrpcScheduler` at it, mirroring the in-process tonic server harness used
/// by `grpc_store_test.rs` / `connection_manager_test.rs`.
async fn setup_grpc_scheduler() -> Result<(MockUpstreamServer, GrpcScheduler), Error> {
    let mock = MockUpstreamServer::default();
    let listener = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_mock = mock.clone();
    background_spawn!("grpc_scheduler_test_server", async move {
        Server::builder()
            .add_service(ExecutionServer::new(server_mock.clone()))
            .add_service(CapabilitiesServer::new(server_mock))
            .serve_with_incoming(listener)
            .await
            .unwrap();
    });

    // Deserialize the spec so every defaulted field (retry: single attempt,
    // no delays; connection counts; keepalives) gets its production default.
    let spec: GrpcSpec = serde_json::from_str(&format!(
        r#"{{"endpoint": {{"address": "grpc://127.0.0.1:{port}"}}}}"#
    ))
    .map_err(|e| {
        Error::new(
            Code::InvalidArgument,
            format!("Failed to deserialize GrpcSpec: {e}"),
        )
    })?;
    Ok((mock, GrpcScheduler::new(&spec)?))
}

/// Builds the `Operation` an RBE server would emit for the given stage,
/// using the same production conversion (`ActionState::as_operation`) that
/// `NativeLink`'s own execution service uses.
fn make_operation(
    stage: ActionStage,
    upstream_operation_id: &str,
    digest: DigestInfo,
) -> Operation {
    let action_state = ActionState {
        client_operation_id: OperationId::from(upstream_operation_id),
        stage,
        action_digest: digest,
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    };
    action_state.as_operation(OperationId::from(upstream_operation_id))
}

const ACTION_DIGEST: DigestInfo = DigestInfo::new([99u8; 32], 512);
const UPSTREAM_OPERATION_ID: &str = "upstream-operation-id";

#[nativelink_test]
async fn grpc_scheduler_add_action_forwards_request_and_streams_updates_test() -> Result<(), Error>
{
    let (mock, scheduler) = setup_grpc_scheduler().await?;

    let tx = mock.script_execute_stream();
    // The initial response must be scripted before `add_action` is awaited:
    // the scheduler consumes it to build the action state result.
    tx.send(Ok(make_operation(
        ActionStage::Queued,
        UPSTREAM_OPERATION_ID,
        ACTION_DIGEST,
    )))
    .unwrap();

    let action_info = make_base_action_info(SystemTime::UNIX_EPOCH, ACTION_DIGEST);
    let mut action_state_result = scheduler
        .add_action(OperationId::from("client-chosen-id"), action_info)
        .await?;

    {
        // The upstream received a faithful translation of the ActionInfo.
        let execute_requests = mock.execute_requests.lock();
        assert_eq!(execute_requests.len(), 1);
        let execute_request = &execute_requests[0];
        assert_eq!(execute_request.instance_name, INSTANCE_NAME);
        assert_eq!(execute_request.action_digest, Some(ACTION_DIGEST.into()));
        // `make_base_action_info` produces a Cacheable action...
        assert!(!execute_request.skip_cache_lookup);
        // ...with the default priority, which must omit the execution policy.
        assert_eq!(execute_request.execution_policy, None);
        assert_eq!(
            execute_request.digest_function,
            i32::from(digest_function::Value::Sha256)
        );
    }

    {
        // The initial state mirrors the upstream operation, and the client
        // operation id is the UPSTREAM operation name (that is the id a
        // client must use to reattach via WaitExecution on the proxy).
        let (action_state, _origin_metadata) = action_state_result.as_state().await?;
        assert_eq!(action_state.stage, ActionStage::Queued);
        assert_eq!(
            action_state.client_operation_id,
            OperationId::from(UPSTREAM_OPERATION_ID)
        );
        assert_eq!(action_state.action_digest, ACTION_DIGEST);
    }

    // The watch channel is created pre-marked as changed so subscribers get
    // the initial state on their first `changed()` call; consume it before
    // scripting further updates so each update maps to one `changed()`.
    let (action_state, _origin_metadata) = action_state_result.changed().await?;
    assert_eq!(action_state.stage, ActionStage::Queued);

    // Follow-up upstream messages stream through as state changes.
    tx.send(Ok(make_operation(
        ActionStage::Executing,
        UPSTREAM_OPERATION_ID,
        ACTION_DIGEST,
    )))
    .unwrap();
    let (action_state, _origin_metadata) = action_state_result.changed().await?;
    assert_eq!(action_state.stage, ActionStage::Executing);

    tx.send(Ok(make_operation(
        ActionStage::Completed(ActionResult {
            exit_code: 7,
            ..ActionResult::default()
        }),
        UPSTREAM_OPERATION_ID,
        ACTION_DIGEST,
    )))
    .unwrap();
    let (action_state, _origin_metadata) = action_state_result.changed().await?;
    match &action_state.stage {
        ActionStage::Completed(action_result) => assert_eq!(action_result.exit_code, 7),
        other => panic!("Expected Completed, got : {other:?}"),
    }

    Ok(())
}

#[nativelink_test]
async fn grpc_scheduler_add_action_upstream_stream_closed_early_errors_test() -> Result<(), Error> {
    let (mock, scheduler) = setup_grpc_scheduler().await?;

    {
        // Error path: the upstream accepts the RPC but closes the stream
        // without ever sending an initial operation.
        let tx = mock.script_execute_stream();
        drop(tx);

        let action_info = make_base_action_info(SystemTime::UNIX_EPOCH, ACTION_DIGEST);
        let Err(err) = scheduler
            .add_action(OperationId::from("client-chosen-id"), action_info)
            .await
        else {
            panic!("Expected add_action to fail when upstream sends no response")
        };
        assert_eq!(err.code, Code::Internal);
        assert!(
            err.to_string()
                .contains("Upstream scheduler didn't accept action"),
            "Unexpected error: {err}"
        );
    }

    {
        // Error path: the upstream closes the stream after the initial
        // response; a pending `changed()` must surface the closure as an
        // error instead of hanging.
        let tx = mock.script_execute_stream();
        tx.send(Ok(make_operation(
            ActionStage::Queued,
            UPSTREAM_OPERATION_ID,
            ACTION_DIGEST,
        )))
        .unwrap();

        let action_info = make_base_action_info(SystemTime::UNIX_EPOCH, ACTION_DIGEST);
        let mut action_state_result = scheduler
            .add_action(OperationId::from("client-chosen-id"), action_info)
            .await?;
        drop(tx);

        // The first `changed()` deterministically re-delivers the initial
        // state (the watch channel is created pre-marked as changed).
        let (action_state, _origin_metadata) = action_state_result.changed().await?;
        assert_eq!(action_state.stage, ActionStage::Queued);

        let Err(err) = action_state_result.changed().await else {
            panic!("Expected changed() to fail when the upstream stream closes")
        };
        assert_eq!(err.code, Code::Internal);
        assert!(
            err.to_string().contains("Channel closed"),
            "Unexpected error: {err}"
        );
    }

    Ok(())
}

#[nativelink_test]
async fn grpc_scheduler_filter_operations_forwards_to_wait_execution_test() -> Result<(), Error> {
    let (mock, scheduler) = setup_grpc_scheduler().await?;

    let tx = mock.script_wait_execution_stream();
    tx.send(Ok(make_operation(
        ActionStage::Executing,
        UPSTREAM_OPERATION_ID,
        ACTION_DIGEST,
    )))
    .unwrap();

    let filter = OperationFilter {
        client_operation_id: Some(OperationId::from(UPSTREAM_OPERATION_ID)),
        ..Default::default()
    };
    let mut stream = scheduler.filter_operations(filter).await?;

    let Some(mut action_state_result) = stream.next().await else {
        panic!("Expected one result in filter_operations stream");
    };
    // The filter's operation id is what gets sent upstream as the
    // WaitExecution operation name.
    {
        let wait_execution_requests = mock.wait_execution_requests.lock();
        assert_eq!(wait_execution_requests.len(), 1);
        assert_eq!(wait_execution_requests[0].name, UPSTREAM_OPERATION_ID);
    }

    let (action_state, _origin_metadata) = action_state_result.as_state().await?;
    assert_eq!(action_state.stage, ActionStage::Executing);
    assert_eq!(
        action_state.client_operation_id,
        OperationId::from(UPSTREAM_OPERATION_ID)
    );

    // Consume the pre-marked initial delivery first (see the add_action
    // test), then stream a follow-up update through.
    let (action_state, _origin_metadata) = action_state_result.changed().await?;
    assert_eq!(action_state.stage, ActionStage::Executing);

    // Updates keep streaming after reattachment.
    tx.send(Ok(make_operation(
        ActionStage::Completed(ActionResult::default()),
        UPSTREAM_OPERATION_ID,
        ACTION_DIGEST,
    )))
    .unwrap();
    let (action_state, _origin_metadata) = action_state_result.changed().await?;
    assert!(matches!(action_state.stage, ActionStage::Completed(_)));

    // The stream contains exactly one entry.
    assert!(stream.next().await.is_none());

    Ok(())
}

#[nativelink_test]
async fn grpc_scheduler_filter_operations_rejects_unsupported_filters_test() -> Result<(), Error> {
    let (_mock, scheduler) = setup_grpc_scheduler().await?;

    {
        // Error path: any filter other than client_operation_id is
        // unsupported by the proxy.
        let filter = OperationFilter {
            client_operation_id: Some(OperationId::from(UPSTREAM_OPERATION_ID)),
            worker_id: Some(WorkerId("some-worker".to_string())),
            ..Default::default()
        };
        let Err(err) = scheduler.filter_operations(filter).await else {
            panic!("Expected unsupported filter to be rejected")
        };
        assert_eq!(err.code, Code::InvalidArgument);
        assert!(
            err.to_string().contains("Unsupported filter"),
            "Unexpected error: {err}"
        );
    }

    {
        // A filter without a client_operation_id cannot be forwarded at all.
        let Err(err) = scheduler
            .filter_operations(OperationFilter::default())
            .await
        else {
            panic!("Expected missing client_operation_id to be rejected")
        };
        assert_eq!(err.code, Code::InvalidArgument);
        assert!(
            err.to_string().contains("client_operation_id"),
            "Unexpected error: {err}"
        );
    }

    Ok(())
}

#[nativelink_test]
async fn grpc_scheduler_filter_operations_upstream_error_yields_empty_stream_test()
-> Result<(), Error> {
    let (mock, scheduler) = setup_grpc_scheduler().await?;

    // No scripted wait_execution stream: the mock upstream fails the RPC.
    // The scheduler deliberately maps upstream lookup failures to an empty
    // result stream (the operation is simply not found) instead of an error.
    let filter = OperationFilter {
        client_operation_id: Some(OperationId::from("unknown-operation")),
        ..Default::default()
    };
    let mut stream = scheduler.filter_operations(filter).await?;
    assert!(stream.next().await.is_none());
    assert_eq!(mock.wait_execution_requests.lock().len(), 1);

    Ok(())
}

#[nativelink_test]
async fn grpc_scheduler_get_known_properties_caches_capabilities_per_instance_test()
-> Result<(), Error> {
    let (mock, scheduler) = setup_grpc_scheduler().await?;
    *mock.supported_node_properties.lock() =
        vec!["OSFamily".to_string(), "container-image".to_string()];

    // First lookup hits the upstream Capabilities service.
    let props = scheduler.get_known_properties("instance-a").await?;
    assert_eq!(
        props,
        vec!["OSFamily".to_string(), "container-image".to_string()]
    );
    {
        let capabilities_requests = mock.capabilities_requests.lock();
        assert_eq!(capabilities_requests.len(), 1);
        assert_eq!(capabilities_requests[0].instance_name, "instance-a");
    }

    // A second lookup for the same instance is served from the cache, even
    // if the upstream answer has changed in the meantime.
    *mock.supported_node_properties.lock() = vec!["changed-upstream".to_string()];
    let props = scheduler.get_known_properties("instance-a").await?;
    assert_eq!(
        props,
        vec!["OSFamily".to_string(), "container-image".to_string()]
    );
    assert_eq!(mock.capabilities_requests.lock().len(), 1);

    // A different instance is a separate cache entry and its own request.
    let props = scheduler.get_known_properties("instance-b").await?;
    assert_eq!(props, vec!["changed-upstream".to_string()]);
    {
        let capabilities_requests = mock.capabilities_requests.lock();
        assert_eq!(capabilities_requests.len(), 2);
        assert_eq!(capabilities_requests[1].instance_name, "instance-b");
    }

    Ok(())
}
