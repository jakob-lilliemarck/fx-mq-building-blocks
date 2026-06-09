use chrono::{DateTime, Utc};
use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::backoff::ExponentialBackoff;

type Inbound = Pin<Box<dyn Stream<Item = String> + Send + 'static>>;

/// Stream that yields `true` when polling should occur.
///
/// Coordinates multiple triggers: exponential backoff, PostgreSQL notifications, and immediate poll overrides.
pub struct PollControlStream {
    inbound: Option<Inbound>,
    failed_attempts: i32,
    reference_time: DateTime<Utc>,
    backoff: ExponentialBackoff,
    poll: bool,
}

impl PollControlStream {
    /// Creates a new poll control stream with the given backoff strategy.
    pub fn new(backoff: ExponentialBackoff) -> Self {
        Self {
            inbound: None,
            failed_attempts: 0,
            reference_time: Utc::now(),
            backoff,
            poll: true, // First poll returns immediately, bypassing backoff
        }
    }

    /// Sets the inbound notification stream.
    ///
    /// When notifications are received, the stream will yield immediately.
    #[tracing::instrument(skip(self, inbound), level = "debug")]
    pub fn with_inbound_stream(
        &mut self,
        inbound: impl Stream<Item = String> + Unpin + Send + 'static,
    ) {
        self.inbound = Some(Box::pin(inbound))
    }

    /// Increments the failed attempts counter.
    ///
    /// Subsequent polls will use exponential backoff based on the attempt count.
    #[tracing::instrument(skip(self), fields(failed_attempts = self.failed_attempts + 1), level = "debug")]
    pub fn increment_failed_attempts(&mut self) {
        self.failed_attempts += 1;
    }

    /// Resets the failed attempts counter to zero.
    ///
    /// Future polls will use regular intervals instead of exponential backoff.
    #[tracing::instrument(skip(self), level = "debug")]
    pub fn reset_failed_attempts(&mut self) {
        self.failed_attempts = 0;
    }

    /// Forces the next poll to return immediately.
    ///
    /// Bypasses all backoff and notification logic for one poll.
    #[tracing::instrument(skip(self), level = "debug")]
    pub fn set_poll(&mut self) {
        self.poll = true
    }

    // Schedules a wakeup after the given duration
    #[tracing::instrument(
        skip(cx),
        fields(duration_ms = duration.as_millis()),
        level = "debug"
    )]
    fn wake_in(cx: &mut Context<'_>, duration: Duration) {
        let waker = cx.waker().clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            waker.wake();
        });
    }

    // Common backoff timing logic - determines if enough time has passed for the next poll
    #[tracing::instrument(
        skip(self, cx),
        fields(attempts = attempts),
        level = "debug"
    )]
    fn handle_backoff_timing(
        &mut self,
        cx: &mut Context<'_>,
        now: DateTime<Utc>,
        attempts: i32,
    ) -> Poll<Option<bool>> {
        let try_at = self.backoff.try_at(attempts, self.reference_time);

        if now >= try_at {
            self.reference_time = now;
            Poll::Ready(Some(true))
        } else {
            let remaining = (try_at - now).to_std().unwrap_or(Duration::ZERO);
            Self::wake_in(cx, remaining);
            Poll::Pending
        }
    }
}

impl Stream for PollControlStream {
    type Item = bool;

    #[tracing::instrument(
        skip(self, cx),
        fields(failed_attempts = self.failed_attempts, poll = self.poll),
        level = "debug"
    )]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let slf = self.get_mut();

        let now = Utc::now();

        // check if there were failed attempts - use exponential backoff
        if slf.failed_attempts > 0 {
            return slf.handle_backoff_timing(cx, now, slf.failed_attempts);
        }

        // check the poll flag
        if slf.poll {
            // set it back to false
            slf.poll = false;
            slf.reference_time = now;
            return Poll::Ready(Some(true));
        }

        // if there is a notification stream, check for notifications
        if let Some(ref mut inbound) = slf.inbound {
            match inbound.as_mut().poll_next(cx) {
                Poll::Ready(Some(_message)) => {
                    // received a Pg notification
                    slf.reference_time = now;
                    return Poll::Ready(Some(true));
                }
                Poll::Ready(None) => {
                    // ignore ended stream
                }
                Poll::Pending => {
                    // ignore pending state
                }
            }
        }

        // fallback: regular polling interval (use base delay for regular polling)
        // Pass attempt=1 to get base_delay (attempt=0 would return immediately)
        // This ensures we poll at regular intervals when no failures or notifications occur
        slf.handle_backoff_timing(cx, now, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_yields_true() {
        let mut stream =
            PollControlStream::new(ExponentialBackoff::new(2, Duration::from_millis(1)));

        // The stream should only ever yield Some(true) and Never Some(false)
        assert_eq!(stream.next().await, Some(true));
        stream.increment_failed_attempts();
        assert_eq!(stream.next().await, Some(true));
    }

    #[tokio::test]
    async fn test_falls_back_to_polling_when_inbound_stream_ends() {
        let duration = Duration::from_millis(5);

        // Create a channel and drop the sender so the stream immediately ends
        let (tx, rx) = futures::channel::mpsc::unbounded::<String>();
        drop(tx);

        let mut stream = PollControlStream::new(ExponentialBackoff::new(2, duration));
        stream.with_inbound_stream(rx);

        let now = Utc::now();

        // First poll: immediate (poll=true)
        assert_eq!(stream.next().await, Some(true));

        // Second poll: should wait for regular backoff (attempt=1, 5ms) and then yield
        // The notification stream has ended, so this proves the fallback works
        assert_eq!(stream.next().await, Some(true));

        let elapsed = (Utc::now() - now).to_std().unwrap_or(Duration::ZERO);
        assert!(
            elapsed >= duration,
            "Expected at least {:?} to have elapsed with a dead notification channel",
            elapsed,
        );
    }

    #[tokio::test]
    async fn test_backoff() {
        let duration = Duration::from_millis(5);

        let mut stream = PollControlStream::new(ExponentialBackoff::new(2, duration));

        let iterations = 3;
        // Iteration 0: immediate (poll=true)
        // Iteration 1: wait 5ms (attempt 1: base_delay * 2^0 = 5ms)
        // Iteration 2: wait 10ms (attempt 2: base_delay * 2^1 = 10ms)
        let mut n = 0;
        let now = Utc::now();
        while let Some(_) = stream.next().await {
            // increment the failed count on each iteration
            stream.increment_failed_attempts();
            if n == iterations - 1 {
                break;
            }
            n += 1;
        }

        let elapsed = (Utc::now() - now).to_std().unwrap_or(Duration::ZERO);
        // First poll: immediate (poll=true), then wait 5ms, then wait 10ms
        // Total expected: ~0ms + 5ms + 10ms = 15ms
        let expected_minimum = duration + duration * 2; // 5ms + 10ms = 15ms
        assert!(
            elapsed >= expected_minimum,
            "Expected elapsed {:?} to be >= {:?} for exponential backoff",
            elapsed,
            expected_minimum
        );
    }

    #[tokio::test]
    async fn test_poll_duration_override() {
        let duration = Duration::from_millis(5);

        let mut stream = PollControlStream::new(ExponentialBackoff::new(2, duration));

        stream.set_poll();

        let now = Utc::now();

        stream.next().await;

        let elapsed = (Utc::now() - now).to_std().unwrap_or(Duration::ZERO);
        assert!(
            elapsed < duration,
            "Expected elapsed to be smaller than duration"
        );
    }
}
