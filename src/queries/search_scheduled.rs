use sqlx::PgExecutor;

/// Returns the number of messages matching `name` and `payload` that are not yet terminal
/// (i.e. not succeeded or dead). This includes pending, in-progress, and retrying messages.
///
/// `payload` is matched with the JSONB containment operator (`@>`), so partial matches are supported.
pub async fn search_scheduled<'tx, E: PgExecutor<'tx>>(
    tx: E,
    name: &str,
    payload: &serde_json::Value,
) -> Result<i64, sqlx::Error> {
    let count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM (
            SELECT id FROM messages_unattempted
            WHERE name = $1 AND payload @> $2

            UNION ALL

            SELECT ma.id FROM messages_attempted ma
            LEFT JOIN attempts_succeeded s ON s.message_id = ma.id
            LEFT JOIN attempts_dead d ON d.message_id = ma.id
            WHERE ma.name = $1
              AND ma.payload @> $2
              AND s.message_id IS NULL
              AND d.message_id IS NULL
        ) matches
        "#,
        name,
        payload
    )
    .fetch_one(tx)
    .await?;

    Ok(count.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backoff::ConstantBackoff,
        models::Message,
        queries::{
            get_next_unattempted, publish_message, report_dead, report_retryable, report_success,
        },
        testing_tools::TestMessage,
    };
    use chrono::Utc;
    use std::time::Duration;
    use uuid::Uuid;

    /// Seeds one message in each non-terminal and terminal state, all with the same name and default payload.
    async fn seed(pool: &sqlx::PgPool) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let backoff = ConstantBackoff::new(Duration::from_mins(5));

        let new_msg = || TestMessage::default().to_raw();

        // Pending: published but never polled
        publish_message(pool, &new_msg()?).await?;

        // In progress: polled, lease still active — leave it there
        publish_message(pool, &new_msg()?).await?;
        get_next_unattempted(pool, now, host_id, hold_for)
            .await?
            .unwrap();

        // Retrying: polled then reported retryable
        publish_message(pool, &new_msg()?).await?;
        let retrying = get_next_unattempted(pool, now, host_id, hold_for)
            .await?
            .unwrap();
        report_retryable(pool, retrying.id, now, 1, backoff.try_at(1, now), "err").await?;

        // Succeeded: polled then reported success
        publish_message(pool, &new_msg()?).await?;
        let succeeded = get_next_unattempted(pool, now, host_id, hold_for)
            .await?
            .unwrap();
        report_success(pool, succeeded.id, now).await?;

        // Dead: polled then reported dead
        publish_message(pool, &new_msg()?).await?;
        let dead = get_next_unattempted(pool, now, host_id, hold_for)
            .await?
            .unwrap();
        report_dead(pool, dead.id, now, "err").await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_zero_when_empty(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let payload = serde_json::to_value(TestMessage::default())?;

        let count = search_scheduled(&pool, TestMessage::NAME, &payload).await?;

        assert_eq!(count, 0);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_counts_pending_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let payload = serde_json::to_value(TestMessage::default())?;
        publish_message(&pool, &TestMessage::default().to_raw()?).await?;

        let count = search_scheduled(&pool, TestMessage::NAME, &payload).await?;

        assert_eq!(count, 1);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_counts_in_progress_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let payload = serde_json::to_value(TestMessage::default())?;

        publish_message(&pool, &TestMessage::default().to_raw()?).await?;
        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .unwrap();

        let count = search_scheduled(&pool, TestMessage::NAME, &payload).await?;

        assert_eq!(count, 1);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_counts_retrying_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let backoff = ConstantBackoff::new(Duration::from_mins(5));
        let payload = serde_json::to_value(TestMessage::default())?;

        publish_message(&pool, &TestMessage::default().to_raw()?).await?;
        let msg = get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .unwrap();
        report_retryable(&pool, msg.id, now, 1, backoff.try_at(1, now), "err").await?;

        let count = search_scheduled(&pool, TestMessage::NAME, &payload).await?;

        assert_eq!(count, 1);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_excludes_succeeded_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        seed(&pool).await?;
        let payload = serde_json::to_value(TestMessage::default())?;

        let count = search_scheduled(&pool, TestMessage::NAME, &payload).await?;

        assert_eq!(count, 3, "succeeded message should not be counted");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_excludes_dead_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        seed(&pool).await?;
        let payload = serde_json::to_value(TestMessage::default())?;

        let count = search_scheduled(&pool, TestMessage::NAME, &payload).await?;

        assert_eq!(count, 3, "dead message should not be counted");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_zero_for_name_mismatch(pool: sqlx::PgPool) -> anyhow::Result<()> {
        seed(&pool).await?;
        let payload = serde_json::to_value(TestMessage::default())?;

        let count = search_scheduled(&pool, "NonExistentMessage", &payload).await?;

        assert_eq!(count, 0);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_zero_for_payload_mismatch(pool: sqlx::PgPool) -> anyhow::Result<()> {
        seed(&pool).await?;
        let payload = serde_json::json!({ "value": 9999 });

        let count = search_scheduled(&pool, TestMessage::NAME, &payload).await?;

        assert_eq!(count, 0);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_matches_on_partial_payload(pool: sqlx::PgPool) -> anyhow::Result<()> {
        seed(&pool).await?;
        // Only supply one field — @> containment should still match
        let payload = serde_json::json!({ "value": 42 });

        let count = search_scheduled(&pool, TestMessage::NAME, &payload).await?;

        assert_eq!(count, 3);
        Ok(())
    }
}
