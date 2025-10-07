use chrono::{DateTime, Utc};
use sqlx::PgExecutor;
use uuid::Uuid;

pub async fn report_dead<'tx, E: PgExecutor<'tx>>(
    tx: E,
    message_id: Uuid,
    now: DateTime<Utc>,
    error: &str,
) -> Result<(), sqlx::Error> {
    let dead_id = Uuid::now_v7();

    sqlx::query!(
        r#"
        WITH del_leases AS (
            DELETE FROM leases
            WHERE message_id = $2
        ),
        del_failed AS (
            DELETE FROM attempts_failed
            WHERE message_id = $2
        ),
        ins_dead AS (
            INSERT INTO attempts_dead (message_id, dead_at)
            VALUES ($2, $3)
        )
        INSERT INTO errors (id, message_id, reported_at, error)
        VALUES ($1, $2, $3, $4)
        "#,
        dead_id,
        message_id,
        now,
        error
    )
    .execute(tx)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queries::{get_next_unattempted, publish_message},
        testing_tools::{TestMessage, is_dead},
    };
    use std::time::Duration;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_deletes_leases_and_failed_attempts(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        get_next_unattempted(&pool, now, host_id, hold_for).await?;

        report_dead(&pool, published.id, now, "some error happend").await?;

        assert!(is_dead(&pool, published.id, now).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_errors_if_the_message_was_not_attempted(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let now = Utc::now();
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        let result = report_dead(&pool, published.id, now, "some error happend").await;

        assert!(result.is_err());

        Ok(())
    }
}
