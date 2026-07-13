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

use core::convert::Into;
use std::collections::HashMap;

use nativelink_config::cas_server::{FetchConfig, WithInstanceName};
use nativelink_error::{Error, ResultExt, make_err, make_input_err};
use nativelink_proto::build::bazel::remote::asset::v1::fetch_server::{
    Fetch, FetchServer as Server,
};
use nativelink_proto::build::bazel::remote::asset::v1::{
    FetchBlobRequest, FetchBlobResponse, FetchDirectoryRequest, FetchDirectoryResponse,
};
use nativelink_proto::google::rpc::Status as GoogleStatus;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::digest_hasher::{default_digest_hasher_func, make_ctx_for_hash_func};
use nativelink_util::store_trait::{Store, StoreLike};
use opentelemetry::context::FutureExt;
use prost::Message;
use tonic::{Code, Request, Response, Status};
use tracing::{Instrument, Level, error, error_span, info, instrument, warn};

use crate::remote_asset_proto::{RemoteAssetArtifact, RemoteAssetQuery};

/// Logs a failed RPC at a level matching who must act on it (issue #1826):
/// request/precondition problems the CLIENT must fix log at WARN, while
/// failures of this server or its backing infrastructure that the OPERATOR
/// must act on log at ERROR. This replaces the blanket
/// `err(level = ...)` that previously logged every error at one level.
///
/// Borderline codes, deliberately classified:
/// - `Aborted` => WARN: a concurrency conflict the client resolves by
///   retrying at a higher level; nothing is broken server-side.
/// - `FailedPrecondition` => WARN: by gRPC contract the client must fix
///   system state before retrying, so it flags a bad request sequence.
/// - `DeadlineExceeded` => ERROR: however the deadline was chosen, this
///   server failed to answer within it; latency on these endpoints is
///   dominated by the backing store, making this an operator signal.
/// - `ResourceExhausted` => ERROR: these endpoints have no per-client
///   quota, so this code only arises from infrastructure backpressure.
///
/// Note: Keep in sync with the copy in `push_server.rs`.
fn log_rpc_failure(rpc: &str, err: &Error) {
    match err.code {
        Code::Cancelled
        | Code::InvalidArgument
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::FailedPrecondition
        | Code::Aborted
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::Unauthenticated => {
            warn!(?err, "{rpc} failed with a client-side error");
        }
        // Unknown, DeadlineExceeded, ResourceExhausted, Internal,
        // Unavailable and DataLoss are operator-actionable.
        _ => error!(?err, "{rpc} failed with a server-side error"),
    }
}

#[derive(Debug, Clone)]
pub struct FetchStoreInfo {
    store: Store,
}

#[derive(Debug, Clone)]
pub struct FetchServer {
    stores: HashMap<String, FetchStoreInfo>,
}

impl FetchServer {
    pub fn new(
        configs: &[WithInstanceName<FetchConfig>],
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(configs.len());
        for config in configs {
            let store = store_manager
                .get_store(&config.fetch_store)
                .ok_or_else(|| {
                    make_input_err!("'fetch_store': '{}' does not exist", config.fetch_store)
                })?;
            stores.insert(config.instance_name.clone(), FetchStoreInfo { store });
        }
        Ok(Self {
            stores: stores.clone(),
        })
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
    }

    async fn inner_fetch_blob(
        &self,
        request: FetchBlobRequest,
    ) -> Result<Response<FetchBlobResponse>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        if request.uris.is_empty() {
            return Err(Error::new(
                Code::InvalidArgument,
                "No uris in fetch request".to_owned(),
            ));
        }
        for uri in &request.uris {
            let asset_request = RemoteAssetQuery::new(uri.clone(), request.qualifiers.clone());
            let asset_digest = asset_request.digest();
            let asset_response_possible = store_info
                .store
                .get_part_unchunked(asset_digest, 0, None)
                .await;

            info!(
                uri = uri,
                digest = format!("{}", asset_digest),
                "Looked up fetch asset"
            );

            if let Ok(asset_response_raw) = asset_response_possible {
                let asset_response = RemoteAssetArtifact::decode(asset_response_raw).unwrap();
                return Ok(Response::new(FetchBlobResponse {
                    status: Some(GoogleStatus {
                        code: Code::Ok.into(),
                        message: "Fetch object found".to_owned(),
                        details: vec![],
                    }),
                    uri: asset_response.uri,
                    qualifiers: asset_response.qualifiers,
                    expires_at: asset_response.expire_at,
                    blob_digest: asset_response.blob_digest,
                    digest_function: asset_response.digest_function,
                }));
            }
        }
        Ok(Response::new(FetchBlobResponse {
            status: Some(make_err!(Code::NotFound, "No item found").into()),
            uri: request.uris.first().cloned().unwrap_or(String::new()),
            qualifiers: vec![],
            expires_at: None,
            blob_digest: None,
            digest_function: default_digest_hasher_func().proto_digest_func().into(),
        }))
    }
}

#[tonic::async_trait]
impl Fetch for FetchServer {
    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        ret(level = Level::DEBUG),
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn fetch_blob(
        &self,
        grpc_request: Request<FetchBlobRequest>,
    ) -> Result<Response<FetchBlobResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        let result = async {
            self.inner_fetch_blob(request)
                .instrument(error_span!("fetch_server_fetch_blob"))
                .with_context(
                    make_ctx_for_hash_func(digest_function)
                        .err_tip(|| "In FetchServer::fetch_blob")?,
                )
                .await
                .err_tip(|| "Failed on fetch_blob() command")
        }
        .await;
        result.map_err(|err| {
            log_rpc_failure("fetch_blob", &err);
            err.into()
        })
    }

    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        ret(level = Level::DEBUG),
        skip_all,
        fields(request = ?_grpc_request.get_ref())
    )]
    async fn fetch_directory(
        &self,
        _grpc_request: Request<FetchDirectoryRequest>,
    ) -> Result<Response<FetchDirectoryResponse>, Status> {
        let err = make_err!(Code::Unimplemented, "FetchDirectory not implemented");
        log_rpc_failure("fetch_directory", &err);
        Err(err.into())
    }
}
