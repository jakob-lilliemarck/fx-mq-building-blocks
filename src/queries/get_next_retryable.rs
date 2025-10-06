use crate::models::RawMessage;
use chrono::{DateTime, Utc};
use sqlx::PgExecutor;
use std::time::Duration;
use uuid::Uuid;

pub async fn get_next_retryable<'tx, E: PgExecutor<'tx>>(
    tx: E,
    now: DateTime<Utc>,
    host_id: Uuid,
    hold_for: Duration,
) -> Result<Option<RawMessage>, sqlx::Error> {
    let expires_at = now + hold_for;

    let message = sqlx::query_as!(
        RawMessage,
        r#"
        WITH next_retryable AS (
            SELECT
                fa.message_id,
                fa.attempted
            FROM attempts_failed fa
            WHERE fa.retry_earliest_at <= $1
              AND NOT EXISTS (
                  SELECT 1 FROM leases l
                  WHERE l.message_id = fa.message_id AND l.expires_at > $1
              )
              AND fa.failed_at = (
                  SELECT MAX(fa2.failed_at)
                  FROM attempts_failed fa2
                  WHERE fa2.message_id = fa.message_id
              )
            ORDER BY fa.failed_at ASC, fa.message_id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        ),
        leased AS (
            INSERT INTO leases (
                message_id,
                acquired_at,
                acquired_by,
                expires_at
                )
            SELECT
                nr.message_id,
                $1,
                $2,
                $3
            FROM next_retryable nr
            RETURNING message_id
        )
        SELECT
            id,
            name,
            hash,
            payload,
            (select attempted from next_retryable) "attempted!:i32"
        FROM messages_attempted
        WHERE id = (SELECT message_id FROM leased);
        "#,
        now,
        host_id,
        expires_at
    )
    .fetch_optional(tx)
    .await?;

    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backoff::ConstantBackoff,
        queries::{get_next_unattempted, publish_message, report_retryable},
        testing_tools::{TestMessage, is_in_progress},
    };

    #[sqlx::test(migrations = "./migrations")]
    async fn it_gets_a_retryable_message(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let backoff = ConstantBackoff::new(Duration::from_mins(0));
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        let try_earliest_at = backoff.try_at(1, now);

        report_retryable(
            &pool,
            published.id,
            now,
            1,
            try_earliest_at,
            "some error happend",
        )
        .await?;

        let polled = get_next_retryable(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected to get a retryable message");

        assert!(polled.id == published.id);
        assert!(is_in_progress(&pool, polled.id, now).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_skips_messages_with_active_leases(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let backoff = ConstantBackoff::new(Duration::from_mins(0));
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        let try_earliest_at = backoff.try_at(1, now);

        report_retryable(
            &pool,
            published.id,
            now,
            1,
            try_earliest_at,
            "some error happend",
        )
        .await?;

        get_next_retryable(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected to get a retryable message");

        let polled = get_next_retryable(&pool, now, host_id, hold_for).await?;

        assert!(polled.is_none());

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_skips_locked_messages(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(0); // set lease duration to 0 for these tests
        let backoff = ConstantBackoff::new(Duration::from_mins(0));
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        let try_earliest_at = backoff.try_at(1, now);

        report_retryable(
            &pool,
            published.id,
            now,
            1,
            try_earliest_at,
            "some error happend",
        )
        .await?;

        let mut tx_1 = pool.begin().await?;
        let mut tx_2 = pool.begin().await?;

        get_next_retryable(&mut *tx_1, now, host_id, hold_for)
            .await?
            .expect("Expected to get a retryable message");

        let polled =
            get_next_retryable(&mut *tx_2, now, host_id, hold_for).await?;

        // close transactions in reverse order
        tx_2.commit().await?;
        tx_1.commit().await?;

        assert!(polled.is_none());

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_none_when_there_is_nothing_to_retry(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let message = TestMessage::default();

        publish_message(&pool, &message.to_raw()?).await?;

        let polled = get_next_retryable(&pool, now, host_id, hold_for).await?;

        assert!(polled.is_none());

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_selects_the_latest_failed_attempt_of_the_message(
        _: sqlx::PgPool
    ) -> anyhow::Result<()> {
        // We must test that we select the previous failure attempt, as we use attempted to incremented the count
        todo!()
    }
}
