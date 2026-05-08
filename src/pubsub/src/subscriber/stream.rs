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
use gaxi::grpc::tonic::{Code as TonicCode, Result as TonicResult, Status as TonicStatus};
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_throttler::CircuitBreaker;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, DropGuard};

pub(super) const INITIAL_DELAY: Duration = Duration::from_millis(100);
pub(super) const MAXIMUM_DELAY: Duration = Duration::from_secs(60);
pub(super) const SERVER_KEEP_ALIVE_MONITOR_INTERVAL: Duration = Duration::from_secs(10);
pub(super) const SERVER_KEEP_ALIVE_TIMEOUT_DURATION: Duration = Duration::from_secs(15);

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

    /// Token cancelled when the server fails to respond within the keepalive window.
    timeout_token: CancellationToken,

    /// Shared timestamp of the last received response.
    last_response: Arc<Mutex<tokio::time::Instant>>,
}

impl<T> TonicStreaming for Stream<T>
where
    T: Stub + 'static,
    <T as Stub>::Stream: TonicStreaming,
{
    async fn next_message(&mut self) -> TonicResult<Option<StreamingPullResponse>> {
        tokio::select! {
            _ = self.timeout_token.cancelled() => {
                Err(TonicStatus::new(
                    TonicCode::Unavailable,
                    "server keepalive timeout",
                ))
            }
            res = self.stream.next_message() => {
                if let Ok(Some(_)) = &res {
                    *self.last_response.lock().unwrap_or_else(|e| e.into_inner()) = tokio::time::Instant::now();
                }
                res
            }
        }
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
    let shutdown = CancellationToken::new();
    keepalive::spawn(request_tx, shutdown.clone());

    let stream = inner
        .streaming_pull(&request_params, request_rx, RequestOptions::default())
        .await?
        .into_inner();

    let timeout_token = CancellationToken::new();
    let last_response = Arc::new(Mutex::new(tokio::time::Instant::now()));

    {
        let last_response = last_response.clone();
        let shutdown = shutdown.clone();
        let timeout_token = timeout_token.clone();
        tokio::spawn(async move {
            loop {
                let elapsed = {
                    let guard = last_response.lock().unwrap_or_else(|e| e.into_inner());
                    guard.elapsed()
                };

                // Signal a keepalive timeout if no response has been received within the timeout window.
                if elapsed >= SERVER_KEEP_ALIVE_TIMEOUT_DURATION {
                    timeout_token.cancel();
                    break;
                }

                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(SERVER_KEEP_ALIVE_MONITOR_INTERVAL) => {}
                }
            }
        });
    }

    Ok(Stream {
        _keepalive_guard: shutdown.drop_guard(),
        stream,
        timeout_token,
        last_response,
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

        // Advance time until stream performs keepalives.
        // To prevent the server keepalive monitor from timing out the stream,
        // we advance time by SERVER_KEEP_ALIVE_MONITOR_INTERVAL and periodically
        // yield server responses until KEEPALIVE_PERIOD.
        let start = tokio::time::Instant::now();
        while start.elapsed() < KEEPALIVE_PERIOD {
            tokio::time::advance(SERVER_KEEP_ALIVE_MONITOR_INTERVAL).await;
            response_tx.send(Ok(test_response(0..0))).await?;
            stream.next_message().await?;
        }

        // Verify the stream performs keepalives.
        assert_eq!(recover_writes_rx.recv().await, Some(keepalive_request()));

        // Verify the bidi nature of the stream continues.
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

    #[tokio_test_no_panics(start_paused = true)]
    async fn server_keepalive_timeout() -> anyhow::Result<()> {
        let (_response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockStub::new();
        mock.expect_streaming_pull()
            .withf(|s, _, _| s == "subscription=projects/my-project/subscriptions/my-subscription")
            .times(1)
            .return_once(move |_s, _r, _o| Ok(TonicResponse::from(response_rx)));

        let mut stream = open_stream(Arc::new(mock), initial_request()).await?;

        // Advance time beyond the server keepalive timeout duration
        tokio::time::advance(SERVER_KEEP_ALIVE_TIMEOUT_DURATION + Duration::from_secs(1)).await;

        let res = stream.next_message().await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.code(), TonicCode::Unavailable);
        assert_eq!(err.message(), "server keepalive timeout");

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn server_keepalive_timeout_reset() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockStub::new();
        mock.expect_streaming_pull()
            .withf(|s, _, _| s == "subscription=projects/my-project/subscriptions/my-subscription")
            .times(1)
            .return_once(move |_s, _r, _o| Ok(TonicResponse::from(response_rx)));

        let mut stream = open_stream(Arc::new(mock), initial_request()).await?;

        // Advance time by less than timeout
        tokio::time::advance(SERVER_KEEP_ALIVE_TIMEOUT_DURATION - Duration::from_secs(1)).await;

        // Send and receive a message to reset the last_response timestamp
        response_tx.send(Ok(test_response(1..10))).await?;
        assert_eq!(stream.next_message().await?, Some(test_response(1..10)));

        // Advance time again by an amount that would have exceeded the original timeout,
        // but is within the new timeout window.
        tokio::time::advance(Duration::from_secs(2)).await;

        // Send another message to confirm the stream is still alive and has not timed out
        response_tx.send(Ok(test_response(11..20))).await?;
        assert_eq!(stream.next_message().await?, Some(test_response(11..20)));

        Ok(())
    }
}
