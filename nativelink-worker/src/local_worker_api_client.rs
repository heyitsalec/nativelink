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

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::unfold;
use hyper::body::Frame;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectWorkerRequest, ExecuteComplete, ExecuteResult, GoingAwayRequest, KeepAliveRequest,
    UpdateForScheduler, UpdateForWorker,
};
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_util::background_spawn;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::encode_stream_proto;
use tokio::sync::mpsc::Sender;
use tonic::codec::{Codec, CompressionEncoding, Streaming};
use tonic::{Response, Status};
use tonic_prost::ProstCodec;

use crate::worker_api_client_wrapper::WorkerApiClientTrait;

/// An in-process implementation of [`WorkerApiClientTrait`] used when a
/// worker's `worker_api_endpoint.uri` uses the `local://<scheduler-name>`
/// scheme (issue #1847). Instead of dialing a TCP/gRPC endpoint, it feeds
/// in-memory streams straight into the wrapped [`WorkerApiServer`]'s
/// stream-based connection handling, so a worker and its scheduler can run
/// in one process with no network transport at all.
///
/// Lifetimes mirror a real gRPC connection: dropping this client's
/// connection (or the worker's update stream ending) makes the server clean
/// up the worker exactly as if a gRPC connection dropped.
#[derive(Debug, Clone)]
pub struct LocalWorkerApiClient {
    server: Arc<WorkerApiServer>,
    channel: Option<Sender<Update>>,
}

impl LocalWorkerApiClient {
    pub const fn new(server: Arc<WorkerApiServer>) -> Self {
        Self {
            server,
            channel: None,
        }
    }

    async fn send_update(&mut self, update: Update) -> Result<(), Error> {
        let tx = self
            .channel
            .as_ref()
            .err_tip(|| "worker update without connect_worker")?;
        match tx.send(update).await {
            Ok(()) => Ok(()),
            Err(_err) => {
                // Remove the sender if it's not going anywhere.
                self.channel.take();
                Err(make_err!(
                    Code::Unavailable,
                    "worker update with disconnected channel"
                ))
            }
        }
    }
}

impl WorkerApiClientTrait for LocalWorkerApiClient {
    async fn connect_worker(
        &mut self,
        request: ConnectWorkerRequest,
    ) -> Result<Response<Streaming<UpdateForWorker>>, Status> {
        drop(self.channel.take());
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        if tx
            .send(Update::ConnectWorkerRequest(request))
            .await
            .is_err()
        {
            return Err(Status::data_loss("Unable to push to newly created channel"));
        }
        self.channel = Some(tx);

        // Worker -> scheduler direction: the same shape the gRPC wrapper
        // sends over the wire, minus the wire.
        let update_stream = Box::pin(unfold(rx, |mut rx| async move {
            let update = rx.recv().await?;
            Some((
                Ok(UpdateForScheduler {
                    update: Some(update),
                }),
                rx,
            ))
        }));

        let mut server_stream = self
            .server
            .connect_worker_in_process(update_stream)
            .await
            .map_err(Status::from)?
            .into_inner();

        // Scheduler -> worker direction: `WorkerApiClientTrait` returns a
        // tonic `Streaming`, which can only be built from a gRPC-framed
        // body. Bridge the server's in-process stream through the gRPC
        // codec over an in-memory channel body (the same mechanism the
        // worker test harness uses). When the server stream ends, the
        // sender drops and the worker's `Streaming` reports end-of-stream,
        // exactly like a closed gRPC connection; when the worker drops its
        // `Streaming`, the send fails and the bridge stops.
        let (frame_tx, body) = ChannelBody::new();
        background_spawn!("local_worker_api_client_bridge", async move {
            while let Some(maybe_update) = server_stream.next().await {
                let Ok(update) = maybe_update else {
                    break;
                };
                let Ok(encoded) = encode_stream_proto(&update) else {
                    break;
                };
                if frame_tx.send(Frame::data(encoded)).await.is_err() {
                    // The worker dropped its end of the stream.
                    break;
                }
            }
        });
        let mut codec = ProstCodec::<UpdateForWorker, UpdateForWorker>::default();
        let stream =
            Streaming::new_request(codec.decoder(), body, Some(CompressionEncoding::Gzip), None);
        Ok(Response::new(stream))
    }

    async fn keep_alive(&mut self, request: KeepAliveRequest) -> Result<(), Error> {
        self.send_update(Update::KeepAliveRequest(request)).await
    }

    async fn going_away(&mut self, request: GoingAwayRequest) -> Result<(), Error> {
        self.send_update(Update::GoingAwayRequest(request)).await
    }

    async fn execution_response(&mut self, request: ExecuteResult) -> Result<(), Error> {
        self.send_update(Update::ExecuteResult(request)).await
    }

    async fn execution_complete(&mut self, request: ExecuteComplete) -> Result<(), Error> {
        self.send_update(Update::ExecuteComplete(request)).await
    }
}
