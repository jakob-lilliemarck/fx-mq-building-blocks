use crate::models::RawMessage;
use chrono::Utc;
use sqlx::PgExecutor;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Message;
    use crate::testing_tools::{TestMessage, is_pending};
    use serde_json::json;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_publishes_a_message(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let message = TestMessage {
            message: "test".to_string(),
            value: 42,
        };
        let published = publish_message(&pool, &message.to_raw()?).await?;

        assert_eq!(TestMessage::NAME, published.name);
        assert_eq!(TestMessage::HASH, published.hash);
        assert_eq!(
            json!({ "message": "test", "value": 42 }),
            published.payload
        );

        assert!(is_pending(&pool, published.id, Utc::now()).await?);

        Ok(())
    }
}
