// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::keepalive;
use super::retry_policy::StreamRetryPolicy;
use super::stub::{Stub, TonicStreaming};
use crate::RequestOptions;
use crate::google::pubsub::v1::{StreamingPullRequest, StreamingPullResponse};
use crate::{Error, Result};
use gaxi::grpc::tonic::Result as TonicResult;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_throttler::CircuitBreaker;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};

pub(super) const INITIAL_DELAY: Duration = Duration::from_millis(100);
pub(super) const MAXIMUM_DELAY: Duration = Duration::from_secs(60);

/// Representation for the `StreamingPull` RPC.
#[derive(Debug)]
pub(super) struct Stream<T>
where
    T: Stub,
{
    /// A guard which signals a shutdown to the task sending keepalive pings
    /// when it is dropped. It is more convenient to hold a `DropGuard` than to
    /// have a custom `impl Drop for Stream`.
    _keepalive_guard: DropGuard,

    /// The stream.
    pub(super) stream: <T as Stub>::Stream,

    pub(super) last_client_ping_time: Arc<std::sync::atomic::AtomicU64>,
    pub(super) last_server_response_time: Arc<std::sync::atomic::AtomicU64>,
    pub(super) anchor: Instant,
    pub(super) timeout_token: CancellationToken,
}

impl<T> TonicStreaming for Stream<T>
where
    T: Stub + 'static,
    <T as Stub>::Stream: TonicStreaming,
{
    async fn next_message(&mut self) -> TonicResult<Option<StreamingPullResponse>> {
        self.stream.next_message().await
    }
}

impl<T> Stream<T>
where
    T: Stub,
{
    /// Open a stream for the `StreamingPull` RPC.
    ///
    /// This method includes retries, and spawns a keepalive task.
    pub(super) async fn new(inner: Arc<T>, initial_req: StreamingPullRequest) -> Result<Self> {
        Self::new_with_backoff(inner, initial_req, default_backoff_policy()).await
    }

    async fn new_with_backoff(
        inner: Arc<T>,
        initial_req: StreamingPullRequest,
        // The default backoff policy is non-deterministic. Exposing the backoff
        // policy in this interface helps us set better test expectations.
        backoff: Arc<dyn BackoffPolicy>,
    ) -> Result<Self> {
        let sleep = async |d| tokio::time::sleep(d).await;
        let attempt = move |_| {
            let inner = inner.clone();
            let initial_req = initial_req.clone();
            async move { open_stream(inner, initial_req).await }
        };

        retry_loop(
            attempt,
            sleep,
            true,
            default_retry_throttler(),
            default_retry_policy(),
            backoff,
        )
        .await
    }
}

/// One attempt to open a stream for the `StreamingPull` RPC.
async fn open_stream<T>(inner: Arc<T>, initial_req: StreamingPullRequest) -> Result<Stream<T>>
where
    T: Stub,
{
    // The only writes we perform are keepalives, which are sent so infrequently
    // that we don't fear any back pressure on this channel.
    let (request_tx, request_rx) = mpsc::channel(1);
    let request_params = format!("subscription={}", initial_req.subscription);

    request_tx.send(initial_req).await.map_err(Error::io)?;

    // Start the keepalive task **before** we open the stream.
    //
    // The future returned by tonic does not yield until the first response has
    // been returned on the stream.[^1]
    //
    // If we do not set up keepalives first, Pub/Sub will close the stream for
    // being idle for ~90s, leading to unnecessary retries.
    //
    // [^1]: https://github.com/hyperium/tonic/issues/515
    let anchor = Instant::now();
    let last_client_ping_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let last_server_response_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let timeout_token = CancellationToken::new();

    let shutdown = CancellationToken::new();
    keepalive::spawn(
        request_tx,
        shutdown.clone(),
        last_client_ping_time.clone(),
        anchor,
    );

    let last_client_ping = last_client_ping_time.clone();
    let last_server_resp = last_server_response_time.clone();
    let timeout_tok = timeout_token.clone();
    let monitor_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = monitor_shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let ping = last_client_ping.load(std::sync::atomic::Ordering::Relaxed);
                    let resp = last_server_resp.load(std::sync::atomic::Ordering::Relaxed);
                    let current_time = Instant::now().duration_since(anchor).as_secs();
                    if ping > resp && (current_time >= ping && current_time - ping >= 15) {
                        timeout_tok.cancel();
                        break;
                    }
                }
            }
        }
    });

    let stream = inner
        .streaming_pull(&request_params, request_rx, RequestOptions::default())
        .await?
        .into_inner();

    Ok(Stream {
        _keepalive_guard: shutdown.drop_guard(),
        stream,
        last_client_ping_time,
        last_server_response_time,
        anchor,
        timeout_token,
    })
}

fn default_retry_policy() -> Arc<StreamRetryPolicy> {
    Arc::new(StreamRetryPolicy)
}

fn default_retry_throttler() -> Arc<Mutex<CircuitBreaker>> {
    // Effectively disable throttling. Opening a stream is done infrequently
    // enough that a throttler is unnecessary.
    Arc::new(Mutex::new(
        CircuitBreaker::new(1000, 0, 0).expect("This is a valid configuration"),
    ))
}

fn default_backoff_policy() -> Arc<ExponentialBackoff> {
    Arc::new(
        ExponentialBackoffBuilder::new()
            .with_initial_delay(INITIAL_DELAY)
            .with_maximum_delay(MAXIMUM_DELAY)
            .with_scaling(4)
            .build()
            .expect("This is a valid configuration"),
    )
}

#[cfg(test)]
mod tests {
    use super::super::keepalive::KEEPALIVE_PERIOD;
    use super::super::lease_state::tests::test_ids;
    use super::super::stub::tests::MockStub;
    use super::*;
    use crate::google::pubsub::v1::{ReceivedMessage, StreamingPullResponse};
    use gaxi::grpc::tonic::Response as TonicResponse;
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_test_macros::tokio_test_no_panics;

    mockall::mock! {
        #[derive(Debug)]
        BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> Duration;
        }
    }

    fn transient_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try again"),
        )
    }

    fn permanent_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::FailedPrecondition)
                .set_message("fail"),
        )
    }

    fn test_response(range: std::ops::Range<i32>) -> StreamingPullResponse {
        StreamingPullResponse {
            received_messages: test_ids(range)
                .into_iter()
                .map(|ack_id| ReceivedMessage {
                    ack_id,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn initial_request() -> StreamingPullRequest {
        StreamingPullRequest {
            subscription: "projects/my-project/subscriptions/my-subscription".to_string(),
            stream_ack_deadline_seconds: 10,
            ..Default::default()
        }
    }

    fn keepalive_request() -> StreamingPullRequest {
        StreamingPullRequest::default()
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);

        let mut mock = MockStub::new();
        mock.expect_streaming_pull()
            .withf(|s, _, _| s == "subscription=projects/my-project/subscriptions/my-subscription")
            .times(1)
            .return_once(move |_s, _r, _o| Ok(TonicResponse::from(response_rx)));

        response_tx.send(Ok(test_response(1..10))).await?;
        response_tx.send(Ok(test_response(11..20))).await?;
        response_tx.send(Ok(test_response(21..30))).await?;
        drop(response_tx);

        let mut stream = open_stream(Arc::new(mock), initial_request()).await?;
        assert_eq!(stream.next_message().await?, Some(test_response(1..10)));
        assert_eq!(stream.next_message().await?, Some(test_response(11..20)));
        assert_eq!(stream.next_message().await?, Some(test_response(21..30)));
        assert_eq!(stream.next_message().await?, None);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn keepalives() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        // We use this channel to surface writes (requests) from outside our
        // mock stream.
        let (recover_writes_tx, mut recover_writes_rx) = mpsc::channel(1);

        let mut mock = MockStub::new();
        mock.expect_streaming_pull()
            .withf(|s, _, _| s == "subscription=projects/my-project/subscriptions/my-subscription")
            .times(1)
            .return_once(move |_s, mut request_rx, _o| {
                tokio::spawn(async move {
                    // Note that this task stays alive as long as we hold
                    // `recover_writes_rx`.
                    while let Some(request) = request_rx.recv().await {
                        recover_writes_tx
                            .send(request)
                            .await
                            .expect("forwarding writes always succeeds");
                    }
                });
                Ok(TonicResponse::from(response_rx))
            });

        let mut stream = open_stream(Arc::new(mock), initial_request()).await?;

        // Verify the stream is seeded with the initial request.
        assert_eq!(recover_writes_rx.recv().await, Some(initial_request()));

        // Verify the stream performs keepalives, even if no messages have been yielded.
        tokio::time::advance(KEEPALIVE_PERIOD).await;
        assert_eq!(recover_writes_rx.recv().await, Some(keepalive_request()));

        // Verify the bidi nature of the stream.
        response_tx.send(Ok(test_response(1..10))).await?;
        assert_eq!(stream.next_message().await?, Some(test_response(1..10)));

        // Shutdown the keepalive task.
        drop(stream);
        assert_eq!(recover_writes_rx.recv().await, None);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn error() -> anyhow::Result<()> {
        let mut mock = MockStub::new();
        mock.expect_streaming_pull()
            .withf(|s, _, _| s == "subscription=projects/my-project/subscriptions/my-subscription")
            .times(1)
            .return_once(|_, _, _| Err(Error::io("fail")));

        let err = open_stream(Arc::new(mock), initial_request())
            .await
            .expect_err("open_stream should fail");
        assert!(err.is_io(), "{err:?}");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn retry_then_success() -> anyhow::Result<()> {
        let mut seq = mockall::Sequence::new();
        let mut mock_stub = MockStub::new();
        let mut mock_backoff = MockBackoffPolicy::new();
        for attempt in 1..20 {
            // Simulate N transient errors + N backoffs. We arbitrarily pick an
            // N > 10 (the default attempt limit for GAPICs).
            mock_stub
                .expect_streaming_pull()
                .withf(|s, _, _| {
                    s == "subscription=projects/my-project/subscriptions/my-subscription"
                })
                .times(1)
                .in_sequence(&mut seq)
                .return_once(|_, _, _| Err(transient_error()));
            mock_backoff
                .expect_on_failure()
                .times(1)
                .withf(move |s| s.attempt_count == attempt)
                .in_sequence(&mut seq)
                .return_const(Duration::ZERO);
        }
        // Simulate a success.
        let (response_tx, response_rx) = mpsc::channel(10);
        response_tx.send(Ok(test_response(1..10))).await?;
        drop(response_tx);

        mock_stub
            .expect_streaming_pull()
            .withf(|s, _, _| s == "subscription=projects/my-project/subscriptions/my-subscription")
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_s, _r, _o| Ok(TonicResponse::from(response_rx)));

        let mut stream = Stream::new_with_backoff(
            Arc::new(mock_stub),
            initial_request(),
            Arc::new(mock_backoff),
        )
        .await?;
        assert_eq!(stream.next_message().await?, Some(test_response(1..10)));
        assert_eq!(stream.next_message().await?, None);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn retry_then_permanent_failure() -> anyhow::Result<()> {
        let mut seq = mockall::Sequence::new();
        let mut mock_stub = MockStub::new();
        let mut mock_backoff = MockBackoffPolicy::new();
        for attempt in 1..20 {
            // Simulate N transient errors + N backoffs. We arbitrarily pick an
            // N > 10 (the default attempt limit for GAPICs).
            mock_stub
                .expect_streaming_pull()
                .withf(|s, _, _| {
                    s == "subscription=projects/my-project/subscriptions/my-subscription"
                })
                .times(1)
                .in_sequence(&mut seq)
                .return_once(|_, _, _| Err(transient_error()));
            mock_backoff
                .expect_on_failure()
                .times(1)
                .withf(move |s| s.attempt_count == attempt)
                .in_sequence(&mut seq)
                .return_const(Duration::ZERO);
        }
        // Simulate a permanent error.
        mock_stub
            .expect_streaming_pull()
            .withf(|s, _, _| s == "subscription=projects/my-project/subscriptions/my-subscription")
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|_, _, _| Err(permanent_error()));
        // The retry loop calculates the backoff delay before determining
        // whether a retry should occur. Hence, we expect this extra call to
        // `on_failure()`.
        mock_backoff
            .expect_on_failure()
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Duration::ZERO);

        let err = Stream::new_with_backoff(
            Arc::new(mock_stub),
            initial_request(),
            Arc::new(mock_backoff),
        )
        .await
        .expect_err("opening stream should fail");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(
            status.code,
            google_cloud_gax::error::rpc::Code::FailedPrecondition
        );
        assert_eq!(status.message, "fail");

        Ok(())
    }
}
