use std::time::Duration;

#[derive(Debug)]
pub struct LinearBackoff {
    base_delay: Duration,
}

impl LinearBackoff {
    pub fn new(base_delay: Duration) -> Self {
        Self { base_delay }
    }
    pub fn try_at(
        &self,
        attempted: u32,
        attempted_at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        attempted_at + self.base_delay * attempted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use std::time::Duration;

    #[test]
    fn it_computes_constant_backoff() {
        let attempted_at =
            DateTime::parse_from_rfc3339("2025-01-01T12:00:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();

        let base_delay = Duration::from_mins(1);
        let backoff = LinearBackoff::new(base_delay);

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
            DateTime::parse_from_rfc3339("2025-01-01T12:03:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();
        let expected_4 =
            DateTime::parse_from_rfc3339("2025-01-01T12:04:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();

        assert_eq!(actual_1, expected_1);
        assert_eq!(actual_2, expected_2);
        assert_eq!(actual_3, expected_3);
        assert_eq!(actual_4, expected_4);
    }
}
