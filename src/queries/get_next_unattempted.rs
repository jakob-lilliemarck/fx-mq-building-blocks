use crate::models::RawMessage;
use crate::queries::Filter;
use chrono::{DateTime, Utc};
use sqlx::PgExecutor;
use std::time::Duration;
use uuid::Uuid;

#[deprecated = "use get_next_unattempted_with_filter(tx, now, host_id, hold_for, Filter::default())"]
pub async fn get_next_unattempted<'tx, E: PgExecutor<'tx>>(
    tx: E,
    now: DateTime<Utc>,
    host_id: Uuid,
    hold_for: Duration,
) -> Result<Option<RawMessage>, sqlx::Error> {
    get_next_unattempted_with_filter(tx, now, host_id, hold_for, Filter::default()).await
}

/// Polls for the next unattempted message, optionally filtering out
/// specified message names.
pub async fn get_next_unattempted_with_filter<'tx, E: PgExecutor<'tx>>(
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
        WITH next_message AS (
            DELETE FROM messages_unattempted
            WHERE id = (
                SELECT id
                FROM messages_unattempted
                WHERE name != ALL($4)
                ORDER BY published_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
        ),
        leased AS (
            INSERT INTO leases (
                message_id,
                acquired_at,
                acquired_by,
                expires_at
            )
            SELECT id, $1, $2, $3
            FROM next_message
            RETURNING message_id
        ),
        attempted AS (
            INSERT INTO messages_attempted (
                id,
                name,
                hash,
                payload,
                published_at
            )
            SELECT
                id,
                name,
                hash,
                payload,
                published_at
            FROM next_message
            RETURNING
                id,
                name,
                hash,
                payload,
                published_at
        )
        SELECT
            id,
            name,
            hash,
            payload,
            0 "attempted!:i32"
        FROM attempted;
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
    use super::*;
    use crate::models::Message;
    use crate::queries::publish_message;
    use crate::testing_tools::{TestMessage, is_in_progress};
    use serde_json::json;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_gets_available_unattempted_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let message = TestMessage {
            message: "testing".to_string(),
            value: 42,
        };

        let published = publish_message(&pool, &message.to_raw()?).await?;

        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let polled = get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message to be returned");

        assert_eq!(published.id, polled.id);
        assert_eq!(TestMessage::NAME, polled.name);
        assert_eq!(TestMessage::HASH, polled.hash);
        assert_eq!(
            json!({ "message": "testing", "value": 42 }),
            published.payload
        );

        assert!(is_in_progress(&pool, published.id, now).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_none_when_none_are_available(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);

        let polled = get_next_unattempted(&pool, now, host_id, hold_for).await?;

        assert!(polled.is_none());

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_gets_an_unattempted_message_once(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let published = publish_message(&pool, &TestMessage::default().to_raw()?).await?;

        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let polled = get_next_unattempted(&pool, now, host_id, hold_for)
            .await?
            .expect("Expected a message to be returned");

        assert!(published.id == polled.id);

        let polled = get_next_unattempted(&pool, now, host_id, hold_for).await?;

        assert!(polled.is_none());

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_skips_locked_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let message_1 = publish_message(&pool, &TestMessage::default().to_raw()?).await?;
        let message_2 = publish_message(&pool, &TestMessage::default().to_raw()?).await?;

        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);

        let mut tx = pool.begin().await?;
        let polled_1 = get_next_unattempted(&mut *tx, now, host_id, hold_for)
            .await?
            .expect("Expected a message to be returned");
        let polled_2 = get_next_unattempted(&mut *tx, now, host_id, hold_for)
            .await?
            .expect("Expected a message to be returned");
        tx.commit().await?;

        assert_eq!(message_1.id, polled_1.id);
        assert_eq!(message_2.id, polled_2.id);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_filters_out_excluded_message_names(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let excluded = RawMessage {
            id: Uuid::now_v7(),
            name: "excluded-job".into(),
            hash: 1,
            payload: json!({}),
            attempted: 0,
        };
        let unfiltered = RawMessage {
            id: Uuid::now_v7(),
            name: "unfiltered-job".into(),
            hash: 2,
            payload: json!({}),
            attempted: 0,
        };

        // Publish excluded first so it gets an earlier published_at.
        // Without the filter it would be returned first by ORDER BY published_at.
        publish_message(&pool, &excluded).await?;
        tokio::time::sleep(Duration::from_micros(1)).await;
        publish_message(&pool, &unfiltered).await?;

        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);

        let filter = Filter::default().with_exclude_names(vec!["excluded-job".into()]);
        let polled = get_next_unattempted_with_filter(&pool, now, host_id, hold_for, filter)
            .await?
            .expect("Expected a message to be returned");

        assert_eq!(polled.id, unfiltered.id);
        assert_eq!(polled.name, "unfiltered-job");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_all_when_exclude_is_empty(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let message_1 = TestMessage::default();
        let message_2 = TestMessage::default();

        let published_1 = publish_message(&pool, &message_1.to_raw()?).await?;
        let published_2 = publish_message(&pool, &message_2.to_raw()?).await?;

        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);

        let polled_1 =
            get_next_unattempted_with_filter(&pool, now, host_id, hold_for, Filter::default())
                .await?
                .expect("Expected first message");

        let polled_2 =
            get_next_unattempted_with_filter(&pool, now, host_id, hold_for, Filter::default())
                .await?
                .expect("Expected second message");

        assert_eq!(polled_1.id, published_1.id);
        assert_eq!(polled_2.id, published_2.id);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_none_when_all_excluded(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let message = TestMessage::default();
        publish_message(&pool, &message.to_raw()?).await?;

        let now = Utc::now();
        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);

        let filter = Filter::default().with_exclude_names(vec![TestMessage::NAME.to_string()]);
        let polled =
            get_next_unattempted_with_filter(&pool, now, host_id, hold_for, filter).await?;

        assert!(polled.is_none());

        Ok(())
    }
}
