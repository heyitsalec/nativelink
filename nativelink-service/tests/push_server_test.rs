// Copyright 2025 The NativeLink Authors. All rights reserved.
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

use std::sync::Arc;

use nativelink_config::cas_server::{PushConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::asset::v1::push_server::Push;
use nativelink_proto::build::bazel::remote::asset::v1::{
    PushBlobRequest, PushBlobResponse, Qualifier,
};
use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_service::push_server::PushServer;
use nativelink_service::remote_asset_proto::RemoteAssetArtifact;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreLike;
use prost::Message;
use sha2::{Digest as Sha2Digest, Sha256};
use tonic::{Code, Request, Status};

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "test_push_store",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

#[nativelink_test]
async fn test_push_blob() -> Result<(), Status> {
    let store_manager = make_store_manager().await?;
    let instance_name = "foo_instance_name".to_string();
    let ps = PushServer::new(
        &[WithInstanceName {
            instance_name: instance_name.clone(),
            config: PushConfig {
                push_store: String::from("test_push_store"),
                read_only: false,
            },
        }],
        &store_manager,
    )
    .expect("PushServer config error");
    let test_hash = Sha256::new();
    let raw_response = ps
        .push_blob(Request::new(PushBlobRequest {
            instance_name,
            uris: vec!["http://1234".to_owned()],
            qualifiers: vec![Qualifier {
                name: "resource_type".to_owned(),
                value: "test".to_owned(),
            }],
            digest_function: 0,
            expire_at: None,
            blob_digest: Some(Digest {
                hash: hex::encode(test_hash.finalize()),
                size_bytes: 4,
            }),
            references_blobs: vec![],
            references_directories: vec![],
        }))
        .await?;
    assert_eq!(raw_response.into_inner(), PushBlobResponse {});

    let push_store = store_manager.get_store("test_push_store").unwrap();

    let digest = DigestInfo::try_new(
        "95eb05bf25e3e14275625ecb8d99bc8a82b4b2afec7334dd696dc19107eb9b05",
        28,
    )?;
    let raw_data = push_store.get_part_unchunked(digest, 0, None).await?;

    let decoded_asset_artifact = RemoteAssetArtifact::decode(raw_data).unwrap();
    assert_eq!(
        decoded_asset_artifact,
        RemoteAssetArtifact::new(
            "http://1234".to_owned(),
            vec![Qualifier {
                name: "resource_type".to_owned(),
                value: "test".to_owned(),
            }],
            Digest {
                hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_string(),
                size_bytes: 4,
            },
            None,
            0,
        )
    );
    Ok(())
}

/// Issue #1826: client-caused failures log at WARN, not ERROR.
#[nativelink_test]
async fn push_blob_client_error_logs_warn() -> Result<(), Error> {
    let store_manager = make_store_manager().await?;
    let ps = PushServer::new(
        &[WithInstanceName {
            instance_name: "foo_instance_name".to_string(),
            config: PushConfig {
                push_store: String::from("test_push_store"),
                // Pushing to a read-only endpoint is a client mistake
                // (PermissionDenied).
                read_only: true,
            },
        }],
        &store_manager,
    )
    .expect("PushServer config error");

    let result = ps
        .push_blob(Request::new(PushBlobRequest {
            instance_name: "foo_instance_name".to_string(),
            ..Default::default()
        }))
        .await;
    let Err(status) = result else {
        panic!("Expected push_blob to fail with PermissionDenied");
    };
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(logs_contain("push_blob failed with a client-side error"));
    assert!(!logs_contain("server-side error"));
    // The instrument macro's `ret` event fires unconditionally when `err(...)`
    // is absent, echoing the Err return value. It must do so at DEBUG (the
    // ac_server.rs precedent), not INFO, or every failure double-logs at the
    // default INFO filter. tracing-test captures all levels regardless of the
    // runtime filter, so the honest assertable invariant is the LEVEL of the
    // echo line, not its absence.
    logs_assert(|lines: &[&str]| {
        match lines
            .iter()
            .filter(|line| line.contains("INFO") && line.contains("return"))
            .count()
        {
            0 => Ok(()),
            n => Err(format!("Expected no INFO-level return echo, got {n}")),
        }
    });

    Ok(())
}

/// Issue #1826: server-side failures log at ERROR, not WARN.
#[nativelink_test]
async fn push_blob_server_error_logs_error() -> Result<(), Error> {
    let store_manager = make_store_manager().await?;
    let ps = PushServer::new(
        &[WithInstanceName {
            instance_name: "foo_instance_name".to_string(),
            config: PushConfig {
                push_store: String::from("test_push_store"),
                read_only: false,
            },
        }],
        &store_manager,
    )
    .expect("PushServer config error");

    let result = ps
        .push_blob(Request::new(PushBlobRequest {
            // An instance this deployment doesn't serve surfaces as Internal
            // (a config/routing mismatch the operator must look at).
            instance_name: "not_configured_instance".to_string(),
            ..Default::default()
        }))
        .await;
    let Err(status) = result else {
        panic!("Expected push_blob to fail for an unconfigured instance");
    };
    assert_eq!(status.code(), Code::Internal);
    assert!(logs_contain("push_blob failed with a server-side error"));
    assert!(!logs_contain("client-side error"));
    // The instrument macro's `ret` event fires unconditionally when `err(...)`
    // is absent, echoing the Err return value. It must do so at DEBUG (the
    // ac_server.rs precedent), not INFO, or every failure double-logs at the
    // default INFO filter. tracing-test captures all levels regardless of the
    // runtime filter, so the honest assertable invariant is the LEVEL of the
    // echo line, not its absence.
    logs_assert(|lines: &[&str]| {
        match lines
            .iter()
            .filter(|line| line.contains("INFO") && line.contains("return"))
            .count()
        {
            0 => Ok(()),
            n => Err(format!("Expected no INFO-level return echo, got {n}")),
        }
    });

    Ok(())
}
