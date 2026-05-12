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

use crate::google::pubsub::v1::StreamingPullRequest;
use tokio::time::{Duration, Instant, interval, interval_at};

pub(super) const KEEPALIVE_PERIOD: Duration = Duration::from_secs(30);

/// Spawns a task to keepalive a stream
///
/// This task periodically writes requests into a channel. The receiver of this
/// channel is the request stream for a StreamingPull bidi RPC.
///
/// Callers may signal a graceful shutdown of this task by cancelling the
/// `CancellationToken` and `await`ing the returned handle.
///
/// Callers can also just drop the returned handle to shutdown.
pub(super) fn spawn(
    request_tx: tokio::sync::mpsc::Sender<crate::google::pubsub::v1::StreamingPullRequest>,
    shutdown: tokio_util::sync::CancellationToken,
    last_response: std::sync::Arc<std::sync::Mutex<tokio::time::Instant>>,
    watchdog_cancel: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut keepalive = interval_at(Instant::now() + KEEPALIVE_PERIOD, KEEPALIVE_PERIOD);
        let mut watchdog = interval(Duration::from_secs(10));
        let mut last_ping_time: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = keepalive.tick() => {
                    let _ = request_tx.send(StreamingPullRequest::default()).await;
                    last_ping_time = Some(Instant::now());
                }
                _ = watchdog.tick() => {
                    if let Some(ping_time) = last_ping_time {
                        let last_resp = *last_response.lock().unwrap();
                        if ping_time > last_resp && ping_time.elapsed() > Duration::from_secs(15) {
                            watchdog_cancel.cancel();
                            break;
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_test_macros::tokio_test_no_panics;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc::channel;
    use tokio_util::sync::CancellationToken;

    #[tokio_test_no_panics(start_paused = true)]
    async fn keepalive_interval() {
        let start = Instant::now();
        let (request_tx, mut request_rx) = channel(1);
        let shutdown = CancellationToken::new();
        let last_response = Arc::new(Mutex::new(Instant::now()));
        let watchdog_cancel = CancellationToken::new();
        let _handle = spawn(request_tx, shutdown, last_response.clone(), watchdog_cancel);

        // Wait for the first keepalive
        let r = request_rx.recv().await.unwrap();
        assert_eq!(r, StreamingPullRequest::default());
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD);
        *last_response.lock().unwrap() = Instant::now();

        // Wait for the second keepalive
        let r = request_rx.recv().await.unwrap();
        assert_eq!(r, StreamingPullRequest::default());
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD * 2);
        *last_response.lock().unwrap() = Instant::now();

        // Wait for the third keepalive
        let r = request_rx.recv().await.unwrap();
        assert_eq!(r, StreamingPullRequest::default());
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD * 3);
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn shutdown_immediately() -> anyhow::Result<()> {
        let start = Instant::now();
        let (request_tx, mut request_rx) = channel(1);
        let shutdown = CancellationToken::new();
        let last_response = Arc::new(Mutex::new(Instant::now()));
        let watchdog_cancel = CancellationToken::new();
        let handle = spawn(
            request_tx,
            shutdown.clone(),
            last_response.clone(),
            watchdog_cancel,
        );

        // Wait for the first keepalive
        let _ = request_rx.recv().await.unwrap();
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD);
        *last_response.lock().unwrap() = Instant::now();

        // Simulate the loop running for a bit.
        const DELTA: Duration = Duration::from_secs(10);
        tokio::time::sleep(DELTA).await;

        // Shutdown the task
        shutdown.cancel();
        handle.await?;

        // Verify that we did not wait for the full keepalive interval.
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD + DELTA);
        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn watchdog_timeout() {
        let (request_tx, mut request_rx) = channel(1);
        let shutdown = CancellationToken::new();
        let last_response = Arc::new(Mutex::new(Instant::now()));
        let watchdog_cancel = CancellationToken::new();
        let handle = spawn(
            request_tx,
            shutdown,
            last_response.clone(),
            watchdog_cancel.clone(),
        );

        // Advance time to trigger the first keepalive.
        tokio::time::sleep(KEEPALIVE_PERIOD).await;
        let _ = request_rx.recv().await.unwrap();

        // Advance time past 15s after keepalive ping to trigger watchdog timeout.
        // Sleeping 21s ensures the watchdog tick at T=50s fully executes before we assert.
        tokio::time::sleep(Duration::from_secs(21)).await;

        assert!(watchdog_cancel.is_cancelled());
        let _ = handle.await;
    }
}
