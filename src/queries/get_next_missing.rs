use crate::models::RawMessage;
use chrono::{DateTime, Utc};
use sqlx::PgExecutor;
use std::time::Duration;
use uuid::Uuid;

/// Gets the nest missing message
/// A message is considered missing when it is attempted but not succeeded or dead and has an expired lease
/// Failed, succeeded and dead messages have no-leases as reporting clears leases.
/// As such attempted messages with expired leases indicate that a worker failed to report before the lease expiry, possibly due to a crash.
pub async fn get_next_missing<'tx, E: PgExecutor<'tx>>(
    tx: E,
    now: DateTime<Utc>,
    host_id: Uuid,
    hold_for: Duration,
) -> Result<Option<RawMessage>, sqlx::Error> {
    let expires_at = now + hold_for;

    let message = sqlx::query_as!(
        RawMessage,
        r#"
        WITH candidate AS (
            SELECT ma.*
            FROM leases l
            JOIN messages_attempted ma
              ON ma.id = l.message_id
            WHERE l.expires_at < $1
              AND NOT EXISTS (
                  SELECT 1 FROM attempts_succeeded s
                  WHERE s.message_id = ma.id
              )
              AND NOT EXISTS (
                SELECT 1 FROM attempts_dead d
                WHERE d.message_id = ma.id
              )
            ORDER BY ma.published_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE leases le
        SET acquired_at = $1,
            acquired_by = $2,
            expires_at = $3
        FROM candidate c
        WHERE le.message_id = c.id
        RETURNING c.id,
            c.name,
            c.hash,
            c.payload,
            0 "attempted!";
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
    use std::time::Duration;

    use chrono::Utc;
    use uuid::Uuid;

    use crate::{
        queries::{
            get_next_missing::get_next_missing, get_next_unattempted,
            publish_message,
        },
        testing_tools::{TestMessage, is_in_progress, is_missing},
    };

    #[sqlx::test(migrations = "./migrations")]
    async fn it_gets_the_next_missing_message(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_millis(1);
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        let polled = get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        // Make sure we wait for lease expiration
        tokio::time::sleep(hold_for * 2).await;

        let current_time = now + hold_for * 2;
        assert!(is_missing(&pool, polled.id, current_time).await?);
        assert!(polled.id == published.id);

        let polled = get_next_missing(&pool, current_time, host_id, hold_for)
            .await?
            .expect("Expected to get a missing message");

        assert!(is_in_progress(&pool, polled.id, current_time).await?);
        assert!(polled.id == published.id);

        Ok(())
    }
}
