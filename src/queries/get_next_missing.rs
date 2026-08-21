use crate::models::RawMessage;
use crate::queries::Filter;
use chrono::{DateTime, Utc};
use sqlx::PgExecutor;
use std::time::Duration;
use uuid::Uuid;

/// Gets the nest missing message
/// A message is considered missing when it is attempted but not succeeded or dead and has an expired lease
/// Failed, succeeded and dead messages have no-leases as reporting clears leases.
/// As such attempted messages with expired leases indicate that a worker failed to report before the lease expiry, possibly due to a crash.
#[deprecated = "use get_next_missing_with_filter(tx, now, host_id, hold_for, Filter::default())"]
pub async fn get_next_missing<'tx, E: PgExecutor<'tx>>(
    tx: E,
    now: DateTime<Utc>,
    host_id: Uuid,
    hold_for: Duration,
) -> Result<Option<RawMessage>, sqlx::Error> {
    get_next_missing_with_filter(tx, now, host_id, hold_for, Filter::default()).await
}

/// Polls for the next missing message, optionally filtering out
/// specified message names.
pub async fn get_next_missing_with_filter<'tx, E: PgExecutor<'tx>>(
    tx: E,
    now: DateTime<Utc>,
    host_id: Uuid,
    hold_for: Duration,
    filter: Filter,
) -> Result<Option<RawMessage>, sqlx::Error> {
    let expires_at = now + hold_for;
    let exclude_names = &filter.exclude_names;

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
              AND ma.name != ALL($4)
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
        expires_at,
        exclude_names as &[String],
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
        models::{Message, RawMessage},
        queries::{
            get_next_missing::{get_next_missing, get_next_missing_with_filter},
            get_next_unattempted, publish_message, Filter,
        },
        testing_tools::{TestMessage, is_in_progress, is_missing},
    };

    #[sqlx::test(migrations = "./migrations")]
    async fn it_gets_the_next_missing_message(pool: sqlx::PgPool) -> anyhow::Result<()> {
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

    #[sqlx::test(migrations = "./migrations")]
    async fn it_filters_out_excluded_message_names_in_missing(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_millis(1);

        let excluded = RawMessage {
            id: Uuid::now_v7(),
            name: "excluded-job".into(),
            hash: 1,
            payload: serde_json::json!({}),
            attempted: 0,
        };
        let unfiltered = RawMessage {
            id: Uuid::now_v7(),
            name: "unfiltered-job".into(),
            hash: 2,
            payload: serde_json::json!({}),
            attempted: 0,
        };

        // Publish excluded first so it gets an earlier published_at.
        // Without the filter it would be returned first by ORDER BY published_at.
        publish_message(&pool, &excluded).await?;
        tokio::time::sleep(Duration::from_micros(1)).await;
        publish_message(&pool, &unfiltered).await?;

        // Move both to in-progress, then let leases expire
        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");
        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        tokio::time::sleep(hold_for * 2).await;

        let current_time = now + hold_for * 2;

        let filter = Filter::default().with_exclude_names(vec!["excluded-job".into()]);
        let polled =
            get_next_missing_with_filter(&pool, current_time, host_id, hold_for, filter)
                .await?
                .expect("Expected a message to be returned");

        assert_eq!(polled.name, "unfiltered-job");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_all_when_exclude_is_empty_in_missing(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_millis(1);
        let message = TestMessage::default();

        let published = publish_message(&pool, &message.to_raw()?).await?;

        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        tokio::time::sleep(hold_for * 2).await;

        let current_time = now + hold_for * 2;

        let result =
            get_next_missing_with_filter(&pool, current_time, host_id, hold_for, Filter::default())
                .await?
                .expect("Expected a message");

        assert_eq!(result.id, published.id);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_none_when_all_excluded_in_missing(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_millis(1);
        let message = TestMessage::default();

        publish_message(&pool, &message.to_raw()?).await?;
        get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message");

        tokio::time::sleep(hold_for * 2).await;

        let current_time = now + hold_for * 2;

        let filter = Filter::default().with_exclude_names(vec![TestMessage::NAME.into()]);
        let polled =
            get_next_missing_with_filter(&pool, current_time, host_id, hold_for, filter).await?;

        assert!(polled.is_none());

        Ok(())
    }
}
