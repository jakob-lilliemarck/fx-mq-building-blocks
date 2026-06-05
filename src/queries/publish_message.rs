use crate::models::RawMessage;
use chrono::Utc;
use sqlx::{PgExecutor, PgTransaction, QueryBuilder};

pub async fn publish_message<'tx, E: PgExecutor<'tx>>(
    tx: E,
    message: &RawMessage,
) -> Result<RawMessage, sqlx::Error> {
    let now = Utc::now();

    let message = sqlx::query_as!(
        RawMessage,
        r#"
        INSERT INTO messages_unattempted (id, name, hash, payload, published_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING
            id,
            name,
            hash,
            payload,
            0 "attempted!:i32"
        "#,
        message.id,
        message.name,
        message.hash,
        message.payload,
        now,
    )
    .fetch_one(tx)
    .await?;

    Ok(message)
}

/// Inserts a single message into `messages_unattempted` and sends a single
/// `pg_notify` on the given channel with payload `"1"`.
///
/// The notification signals that new work is available — it carries the count
/// of inserted messages (always `1` for this function) rather than individual
/// message IDs.
pub async fn publish_message_with_notify(
    tx: &mut PgTransaction<'_>,
    message: &RawMessage,
    channel: &str,
) -> Result<RawMessage, sqlx::Error> {
    let now = Utc::now();

    let message = sqlx::query_as!(
        RawMessage,
        r#"
        INSERT INTO messages_unattempted (id, name, hash, payload, published_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING
            id,
            name,
            hash,
            payload,
            0 "attempted!:i32"
        "#,
        message.id,
        message.name,
        message.hash,
        message.payload,
        now,
    )
    .fetch_one(&mut **tx)
    .await?;

    sqlx::query("SELECT pg_notify($1, $2::text)")
        .bind(channel)
        .bind(1i64)
        .execute(&mut **tx)
        .await?;

    Ok(message)
}

/// Inserts multiple messages into `messages_unattempted` in a single batch
/// and sends a **single** `pg_notify` on the given channel with the total
/// count as payload (e.g. `"5"` for 5 messages).
///
/// As with [`publish_message_with_notify`], there is exactly one NOTIFY per
/// call, regardless of how many messages are inserted.
///
/// Returns an empty `Vec` when `messages` is empty — no NOTIFY is sent in
/// that case.
pub async fn publish_many_messages_with_notify(
    tx: &mut PgTransaction<'_>,
    messages: &[RawMessage],
    channel: &str,
) -> Result<Vec<RawMessage>, sqlx::Error> {
    if messages.is_empty() {
        return Ok(Vec::new());
    }

    let now = Utc::now();
    let mut query_builder = QueryBuilder::new(
        "INSERT INTO messages_unattempted (id, name, hash, payload, published_at) VALUES ",
    );

    let mut first = true;
    for msg in messages {
        if !first {
            query_builder.push(", ");
        }
        first = false;
        query_builder
            .push("(")
            .push_bind(msg.id)
            .push(", ")
            .push_bind(&msg.name)
            .push(", ")
            .push_bind(msg.hash)
            .push(", ")
            .push_bind(&msg.payload)
            .push(", ")
            .push_bind(&now)
            .push(")");
    }

    let published: Vec<RawMessage> = query_builder
        .push(" RETURNING id, name, hash, payload")
        .build()
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|row| {
            use sqlx::Row;
            RawMessage {
                id: row.get("id"),
                name: row.get("name"),
                hash: row.get("hash"),
                payload: row.get("payload"),
                attempted: 0,
            }
        })
        .collect();

    if !published.is_empty() {
        let count = published.len() as i64;
        sqlx::query("SELECT pg_notify($1, $2::text)")
            .bind(channel)
            .bind(count)
            .execute(&mut **tx)
            .await?;
    }

    Ok(published)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Message;
    use crate::testing_tools::{TestMessage, is_pending};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_publishes_a_message(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let message = TestMessage {
            message: "test".to_string(),
            value: 42,
        };
        let published = publish_message(&pool, &message.to_raw()?).await?;

        assert_eq!(TestMessage::NAME, published.name);
        assert_eq!(TestMessage::HASH, published.hash);
        assert_eq!(json!({ "message": "test", "value": 42 }), published.payload);

        assert!(is_pending(&pool, published.id, Utc::now()).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_publishes_and_notifies(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let raw = TestMessage::default().to_raw()?;

        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener.listen("test_channel").await?;
        let mut notifications = listener.into_stream();

        let mut tx = pool.begin().await?;
        let published = publish_message_with_notify(&mut tx, &raw, "test_channel").await?;
        tx.commit().await?;

        assert!(is_pending(&pool, published.id, Utc::now()).await?);

        let notification = notifications
            .next()
            .await
            .expect("expected a pg_notify to be received")?;
        assert_eq!(notification.payload(), "1");
        assert_eq!(notification.channel(), "test_channel");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_publishes_multiple_messages_with_notify(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let messages = vec![
            TestMessage::new("one".to_string(), 1).to_raw()?,
            TestMessage::new("two".to_string(), 2).to_raw()?,
            TestMessage::new("three".to_string(), 3).to_raw()?,
        ];

        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener.listen("test_channel").await?;
        let mut notifications = listener.into_stream();

        let mut tx = pool.begin().await?;
        let published =
            publish_many_messages_with_notify(&mut tx, &messages, "test_channel").await?;
        tx.commit().await?;

        assert_eq!(published.len(), 3);
        for msg in &published {
            assert_eq!(msg.name, TestMessage::NAME);
            assert!(is_pending(&pool, msg.id, Utc::now()).await?);
        }

        let notification = notifications
            .next()
            .await
            .expect("expected a pg_notify to be received")?;
        assert_eq!(notification.payload(), "3");
        assert_eq!(notification.channel(), "test_channel");

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_empty_for_no_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener.listen("test_channel").await?;
        let mut notifications = listener.into_stream();

        let mut tx = pool.begin().await?;
        let published = publish_many_messages_with_notify(&mut tx, &[], "test_channel").await?;
        tx.commit().await?;

        assert!(published.is_empty());

        tokio::time::timeout(Duration::from_millis(100), notifications.next())
            .await
            .expect_err("expected no notification for empty publish");

        Ok(())
    }
}
