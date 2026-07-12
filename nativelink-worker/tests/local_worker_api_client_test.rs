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

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_config::cas_server::{EndpointConfig, LocalWorkerConfig, WorkerApiConfig};
use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, ActionUniqueKey, ActionUniqueQualifier,
    OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::{ActionStateResult, ClientStateManager};
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_worker::local_worker::LocalWorker;
use nativelink_worker::local_worker_api_client::LocalWorkerApiClient;
use pretty_assertions::assert_eq;
use tokio::sync::{Notify, broadcast};
use utils::mock_running_actions_manager::{MockRunningAction, MockRunningActionsManager};

mod utils {
    // Shared with other worker test binaries; this binary uses a subset.
    #[allow(dead_code)]
    pub(crate) mod mock_running_actions_manager;
}

const SCHEDULER_NAME: &str = "main_scheduler";
const INSTANCE_NAME: &str = "foo_instance_name";
const NOW_TIME: u64 = 10000;

struct TestContext {
    scheduler: Arc<SimpleScheduler>,
    client: LocalWorkerApiClient,
    actions_manager: Arc<MockRunningActionsManager>,
    worker_drop_guard: JoinHandleDropGuard<Result<(), Error>>,
}

/// Builds a real `SimpleScheduler`, wraps it in a real `WorkerApiServer`,
/// and runs a real `LocalWorker` loop against it through the in-process
/// `local://` transport. Only action execution itself is mocked.
async fn setup_local_transport_worker() -> TestContext {
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            // High enough that a disconnect requeues instead of erroring out.
            max_job_retries: 10,
            ..Default::default()
        },
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
    );

    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
        },
        &HashMap::from([(SCHEDULER_NAME.to_string(), worker_scheduler)]),
        Box::new(|| Ok(Duration::from_secs(NOW_TIME))),
        [0; 6],
    )
    .expect("Failed to create WorkerApiServer");
    let client = LocalWorkerApiClient::new(Arc::new(worker_api_server));

    let actions_manager = Arc::new(MockRunningActionsManager::new());
    let worker = LocalWorker::new_with_connection_factory_and_actions_manager(
        Arc::new(LocalWorkerConfig {
            worker_api_endpoint: EndpointConfig {
                uri: format!("local://{SCHEDULER_NAME}"),
                timeout: Some(10000.),
                tls_config: None,
            },
            ..Default::default()
        }),
        actions_manager.clone(),
        {
            let client = client.clone();
            Box::new(move || {
                let client = client.clone();
                Box::pin(async move { Ok(client) })
            })
        },
        Box::new(|_duration| Box::pin(async move { /* No sleep */ })),
    );
    let (shutdown_tx, _) = broadcast::channel(1);
    let worker_drop_guard = spawn!("local_transport_worker", async move {
        worker.run(shutdown_tx.subscribe()).await
    });

    TestContext {
        scheduler,
        client,
        actions_manager,
        worker_drop_guard,
    }
}

fn make_action_info(action_digest: DigestInfo) -> Arc<ActionInfo> {
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([1u8; 32], 10),
        input_root_digest: DigestInfo::new([2u8; 32], 10),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: UNIX_EPOCH,
        insert_timestamp: SystemTime::now(),
        unique_qualifier: ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: action_digest,
        }),
    })
}

/// Waits until the action reaches a state matching `want`, skipping
/// intermediate states (the listener delivers the initial Queued state
/// first, and the exact number of observable transitions depends on timing
/// between the matcher and this test).
async fn wait_for_stage(
    action_listener: &mut Box<dyn ActionStateResult>,
    want: fn(&ActionStage) -> bool,
) -> Result<Arc<ActionState>, Error> {
    loop {
        let (action_state, _origin_metadata) = action_listener.changed().await?;
        if want(&action_state.stage) {
            return Ok(action_state);
        }
    }
}

/// Drives the mocked execution of the currently-assigned action to a
/// successful completion through the real worker loop and local transport.
async fn complete_current_action(
    actions_manager: &Arc<MockRunningActionsManager>,
) -> Result<(), Error> {
    let action_result = ActionResult {
        exit_code: 0,
        ..ActionResult::default()
    };
    let running_action = Arc::new(MockRunningAction::new());
    actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;
    running_action
        .simple_expect_get_finished_result(Ok(action_result))
        .await?;
    actions_manager.expect_cache_action_result().await;
    Ok(())
}

/// Issue #1847: a worker connected through the in-process `local://`
/// transport registers with the scheduler and executes an action end to
/// end -- `add_action`, `StartExecute` delivery, execution response, and
/// client-visible Completed state -- with no TCP/gRPC involved.
#[nativelink_test]
async fn local_transport_worker_executes_action_test() -> Result<(), Error> {
    let test_context = setup_local_transport_worker().await;

    let mut action_listener = test_context
        .scheduler
        .add_action(
            OperationId::default(),
            make_action_info(DigestInfo::new([99u8; 32], 512)),
        )
        .await?;

    // The scheduler matched the action to the worker over the local
    // transport.
    wait_for_stage(&mut action_listener, |stage| {
        matches!(stage, ActionStage::Executing)
    })
    .await?;

    complete_current_action(&test_context.actions_manager).await?;

    // The execution response travelled back over the local transport.
    let action_state = wait_for_stage(&mut action_listener, |stage| {
        matches!(stage, ActionStage::Completed(_))
    })
    .await?;
    match &action_state.stage {
        ActionStage::Completed(action_result) => assert_eq!(action_result.exit_code, 0),
        other => panic!("Expected Completed, got : {other:?}"),
    }

    Ok(())
}

/// Issue #1847, the load-bearing lifetime test: dropping the worker's side
/// of the in-process connection must clean up the scheduler-side worker
/// state exactly like a gRPC connection drop -- the worker is evicted, its
/// in-flight action is requeued, and a fresh worker can pick it up.
#[nativelink_test]
async fn local_transport_disconnect_cleans_up_worker_test() -> Result<(), Error> {
    let test_context = setup_local_transport_worker().await;

    let mut action_listener = test_context
        .scheduler
        .add_action(
            OperationId::default(),
            make_action_info(DigestInfo::new([99u8; 32], 512)),
        )
        .await?;
    wait_for_stage(&mut action_listener, |stage| {
        matches!(stage, ActionStage::Executing)
    })
    .await?;

    // Kill the worker task. This drops the client's update channel, which
    // ends the update stream inside WorkerApiServer -- the same signal a
    // dropped gRPC connection produces.
    drop(test_context.worker_drop_guard);

    // The scheduler must evict the worker and requeue the in-flight action.
    wait_for_stage(&mut action_listener, |stage| {
        matches!(stage, ActionStage::Queued)
    })
    .await?;

    // A fresh worker over a fresh local connection picks the action up,
    // proving the dead worker's registration is fully gone (were it still
    // registered, its broken update channel would be selected and the
    // action would not reach the new worker).
    let actions_manager = Arc::new(MockRunningActionsManager::new());
    let worker = LocalWorker::new_with_connection_factory_and_actions_manager(
        Arc::new(LocalWorkerConfig {
            worker_api_endpoint: EndpointConfig {
                uri: format!("local://{SCHEDULER_NAME}"),
                timeout: Some(10000.),
                tls_config: None,
            },
            ..Default::default()
        }),
        actions_manager.clone(),
        {
            let client = test_context.client.clone();
            Box::new(move || {
                let client = client.clone();
                Box::pin(async move { Ok(client) })
            })
        },
        Box::new(|_duration| Box::pin(async move { /* No sleep */ })),
    );
    let (shutdown_tx, _) = broadcast::channel(1);
    let _second_worker_guard = spawn!("local_transport_worker_2", async move {
        worker.run(shutdown_tx.subscribe()).await
    });

    wait_for_stage(&mut action_listener, |stage| {
        matches!(stage, ActionStage::Executing)
    })
    .await?;
    complete_current_action(&actions_manager).await?;
    wait_for_stage(&mut action_listener, |stage| {
        matches!(stage, ActionStage::Completed(_))
    })
    .await?;

    Ok(())
}
