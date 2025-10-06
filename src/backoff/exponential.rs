use chrono::{DateTime, Utc};
use std::time::Duration;

#[derive(Debug)]
pub struct ExponentialBackoff {
    base: u32,
    base_delay: Duration,
}

impl ExponentialBackoff {
    pub fn new(
        base: u32,
        base_delay: Duration,
    ) -> Self {
        Self { base, base_delay }
    }

    pub fn try_at(
        &self,
        attempted: i32,
        attempted_at: DateTime<Utc>,
    ) -> DateTime<Utc> {
        if attempted <= 0 {
            attempted_at // No delay for zero attempts
        } else {
            let attempted = attempted as u32;
            attempted_at + self.base_delay * self.base.pow(attempted - 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_computes_exponential_backoff_with_base_2() {
        let attempted_at =
            DateTime::parse_from_rfc3339("2025-01-01T12:00:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();

        let base: u32 = 2;
        let base_delay = Duration::from_mins(1);
        let backoff = ExponentialBackoff::new(base, base_delay);

        let actual_1 = backoff.try_at(1, attempted_at);
        let actual_2 = backoff.try_at(2, attempted_at);
        let actual_3 = backoff.try_at(3, attempted_at);
        let actual_4 = backoff.try_at(4, attempted_at);

        let expected_1 =
            DateTime::parse_from_rfc3339("2025-01-01T12:01:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();
        let expected_2 =
            DateTime::parse_from_rfc3339("2025-01-01T12:02:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();
        let expected_3 =
            DateTime::parse_from_rfc3339("2025-01-01T12:04:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();
        let expected_4 =
            DateTime::parse_from_rfc3339("2025-01-01T12:08:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();

        assert_eq!(actual_1, expected_1);
        assert_eq!(actual_2, expected_2);
        assert_eq!(actual_3, expected_3);
        assert_eq!(actual_4, expected_4);
    }

    #[test]
    fn it_handles_zero_attempts() {
        let attempted_at =
            DateTime::parse_from_rfc3339("2025-01-01T12:00:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();

        let backoff = ExponentialBackoff::new(2, Duration::from_mins(1));

        let actual = backoff.try_at(0, attempted_at);

        // Zero attempts should return the same timestamp (no delay)
        assert_eq!(actual, attempted_at);
    }
}
