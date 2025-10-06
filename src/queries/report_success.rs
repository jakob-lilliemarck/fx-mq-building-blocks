use chrono::{DateTime, Utc};
use sqlx::PgExecutor;
use uuid::Uuid;

pub async fn report_success<'tx, E: PgExecutor<'tx>>(
    tx: E,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        WITH del_leases AS (
            DELETE FROM leases
            WHERE message_id = $1
        ),
        del_failed AS (
            DELETE FROM attempts_failed
            WHERE message_id = $1
        )
        INSERT INTO attempts_succeeded (message_id, succeeded_at)
        VALUES ($1, $2);
        "#,
        message_id,
        now,
    )
    .execute(tx)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backoff::ConstantBackoff;
    use crate::queries::{
        get_next_retryable, get_next_unattempted, publish_message,
        report_retryable,
    };
    use crate::testing_tools::{TestMessage, is_succeeded};
    use std::time::Duration;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_deletes_associated_leases(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        let polled = get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        assert!(published.id == polled.id);

        report_success(&pool, polled.id, now).await?;

        assert!(is_succeeded(&pool, published.id, now).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_errors_if_the_message_was_not_attempted(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        let result = report_success(&pool, published.id, now).await;

        assert!(result.is_err());

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_deletes_failed_attempts(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let backoff = ConstantBackoff::new(Duration::from_mins(0)); // zero backoff
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        let try_earliest_at = backoff.try_at(1, now);

        report_retryable(&pool, published.id, now, 1, try_earliest_at, "error")
            .await?;

        get_next_retryable(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        report_success(&pool, published.id, now).await?;

        assert!(is_succeeded(&pool, published.id, now).await?);

        Ok(())
    }
}
