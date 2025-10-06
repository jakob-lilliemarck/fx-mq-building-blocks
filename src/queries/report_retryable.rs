use chrono::{DateTime, Utc};
use sqlx::PgExecutor;
use uuid::Uuid;

pub async fn report_retryable<'tx, E: PgExecutor<'tx>>(
    tx: E,
    message_id: Uuid,
    attempted_at: DateTime<Utc>,
    attempted: i32, // increment this before passing to the query!
    retry_earliest_at: DateTime<Utc>,
    error: &str,
) -> Result<(), sqlx::Error> {
    let failed_id = Uuid::now_v7();
    let error_id = Uuid::now_v7();

    sqlx::query!(
        r#"
        WITH del_leases AS (
            DELETE FROM leases
            WHERE message_id = $1
        ),
        ins_failed AS (
            INSERT INTO attempts_failed (
                id,
                message_id,
                failed_at,
                attempted,
                retry_earliest_at
            )
            VALUES ($2, $1, $3, $4, $5)
        )
        INSERT INTO errors (
            id,
            message_id,
            reported_at,
            error
        )
        VALUES ($6, $1, $3, $7)
        "#,
        message_id,        // $1 → message_id
        failed_id,         // $2 → new failed row ID
        attempted_at,      // $3 → failed_at / reported_at
        attempted,         // $4 → attempted
        retry_earliest_at, // $5 → retry_earliest_at
        error_id,          // $6 → error row ID
        error              // $7 → error text
    )
    .execute(tx)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backoff::ConstantBackoff,
        queries::{get_next_unattempted, publish_message},
        testing_tools::{TestMessage, is_failed},
    };
    use std::time::Duration;
    use uuid::Uuid;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_deletes_leases_and_failed_attempts(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let message = TestMessage::default();
        let backoff = ConstantBackoff::new(Duration::from_mins(5));

        let published = publish_message(&pool, &message.to_raw()?).await?;

        get_next_unattempted(&pool, now, host_id, hold_for).await?;

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

        assert!(is_failed(&pool, published.id, now).await?);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_errors_if_the_message_was_not_attempted(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let message = TestMessage::default();
        let backoff = ConstantBackoff::new(Duration::from_mins(5));

        let published = publish_message(&pool, &message.to_raw()?).await?;

        let try_earliest_at = backoff.try_at(1, now);

        let result = report_retryable(
            &pool,
            published.id,
            now,
            1,
            try_earliest_at,
            "some error happend",
        )
        .await;

        assert!(result.is_err());

        Ok(())
    }
}
