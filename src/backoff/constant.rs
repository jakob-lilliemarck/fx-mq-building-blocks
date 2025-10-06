use std::time::Duration;

#[derive(Debug)]
pub struct ConstantBackoff {
    base_delay: Duration,
}

impl ConstantBackoff {
    pub fn new(base_delay: Duration) -> Self {
        Self { base_delay }
    }

    pub fn try_at(
        &self,
        _: i32,
        attempted_at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        attempted_at + self.base_delay
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    #[test]
    fn it_computes_constant_backoff() {
        let attempted_at =
            DateTime::parse_from_rfc3339("2025-01-01T12:00:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();

        let base_delay = Duration::from_mins(1);
        let backoff = ConstantBackoff::new(base_delay);

        let actual_1 = backoff.try_at(1, attempted_at);
        let actual_2 = backoff.try_at(2, attempted_at);

        let expected =
            DateTime::parse_from_rfc3339("2025-01-01T12:01:00-00:00")
                .expect("Expected to parse the timestsamp")
                .to_utc();

        assert_eq!(actual_1, expected);
        assert_eq!(actual_2, expected);
    }
}
