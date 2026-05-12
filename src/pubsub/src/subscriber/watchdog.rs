// Copyright 2026 Google LLC
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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub(super) const SERVER_KEEP_ALIVE_TIMEOUT_DURATION: Duration = Duration::from_secs(15);

/// Tracks server response validation timestamps and monitors connection stream inactivity
#[derive(Debug)]
pub(super) struct KeepaliveResponseWatchdog {
    last_response_timestamp: Arc<AtomicU64>,
    timeout_token: CancellationToken,
}

impl KeepaliveResponseWatchdog {
    /// Allocates a new watchdog response tracker instance
    #[allow(dead_code)]
    pub(super) fn new(timeout_token: CancellationToken) -> Self {
        let now = Instant::now().elapsed().as_millis() as u64;
        Self {
            last_response_timestamp: Arc::new(AtomicU64::new(now)),
            timeout_token,
        }
    }

    /// Refreshes the internal validation timestamp upon extracting server communication responses
    #[allow(dead_code)]
    pub(super) fn refresh_validation_timestamp(&self) {
        let now = Instant::now().elapsed().as_millis() as u64;
        self.last_response_timestamp.store(now, Ordering::Release);
    }

    /// Spawns an un-intrusive background monitor timer tracking communication inactivity
    #[allow(dead_code)]
    pub(super) fn spawn_watchdog_monitor(&self) {
        let last_response = Arc::clone(&self.last_response_timestamp);
        let timeout_token = self.timeout_token.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = timeout_token.cancelled() => break,
                    _ = interval.tick() => {
                        let last = last_response.load(Ordering::Acquire);
                        let current = Instant::now().elapsed().as_millis() as u64;
                        let elapsed = Duration::from_millis(current.saturating_sub(last));

                        // Signal a keepalive timeout if no response has been received within the tolerance window
                        if elapsed >= SERVER_KEEP_ALIVE_TIMEOUT_DURATION {
                            timeout_token.cancel();
                            break;
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn test_watchdog_timeout_cancellation() {
        let token = CancellationToken::new();
        let watchdog = KeepaliveResponseWatchdog::new(token.clone());
        watchdog.spawn_watchdog_monitor();

        // Forcefully set timestamp deep into the past to trigger immediate timeout
        watchdog.last_response_timestamp.store(0, Ordering::Release);
        tokio::time::sleep(Duration::from_secs(6)).await;

        assert!(token.is_cancelled());
    }
}
