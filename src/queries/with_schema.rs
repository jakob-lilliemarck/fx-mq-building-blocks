use crate::models::RawMessage;
use crate::queries::{
    get_next_missing, get_next_retryable, get_next_unattempted, publish_message, report_dead,
    report_retryable, report_success, request_lease,
};
use crate::testing_tools::{
    is_dead, is_failed, is_in_progress, is_missing, is_pending, is_succeeded,
};
use chrono::{DateTime, Utc};
use sqlx::PgTransaction;
use std::time::Duration;
use uuid::Uuid;

/// Sets the schema for the given transaction.
/// This should be called before running any queries that need to operate on a specific schema.
pub async fn set_schema_for_transaction(
    tx: &mut PgTransaction<'_>,
    schema: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("SET LOCAL search_path TO {}", schema))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[derive(Debug)]
pub struct Queries {
    schema: String,
}

impl Queries {
    pub fn new(schema: &str) -> Self {
        Self {
            schema: schema.to_string(),
        }
    }

    pub async fn get_next_retryable(
        &self,
        tx: &mut PgTransaction<'_>,
        now: DateTime<Utc>,
        host_id: Uuid,
        hold_for: Duration,
    ) -> Result<Option<RawMessage>, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        get_next_retryable(&mut **tx, now, host_id, hold_for).await
    }

    pub async fn get_next_missing<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        now: DateTime<Utc>,
        host_id: Uuid,
        hold_for: Duration,
    ) -> Result<Option<RawMessage>, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        get_next_missing(&mut **tx, now, host_id, hold_for).await
    }

    pub async fn get_next_unattempted<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        now: DateTime<Utc>,
        host_id: Uuid,
        hold_for: Duration,
    ) -> Result<Option<RawMessage>, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        get_next_unattempted(&mut **tx, now, host_id, hold_for).await
    }

    pub async fn publish_message(
        &self,
        tx: &mut PgTransaction<'_>,
        message: RawMessage,
    ) -> Result<RawMessage, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        publish_message(&mut **tx, &message).await
    }

    pub async fn report_dead<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
        error_str: &str,
    ) -> Result<(), sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        report_dead(&mut **tx, message_id, now, error_str).await
    }

    pub async fn report_retryable<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        failed_at: DateTime<Utc>,
        attempted: i32, // increment this before passing to the query!
        try_earliest_at: DateTime<Utc>,
        error_str: &str,
    ) -> Result<(), sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        report_retryable(
            &mut **tx,
            message_id,
            failed_at,
            attempted,
            try_earliest_at,
            error_str,
        )
        .await
    }

    pub async fn report_success<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        report_success(&mut **tx, message_id, now).await
    }

    pub async fn request_lease<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
        host_id: Uuid,
        hold_for: Duration,
    ) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        request_lease(&mut **tx, message_id, now, host_id, hold_for).await
    }

    pub async fn is_pending<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        is_pending(&mut **tx, message_id, now).await
    }

    pub async fn is_in_progress<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        is_in_progress(&mut **tx, message_id, now).await
    }

    pub async fn is_missing<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        is_missing(&mut **tx, message_id, now).await
    }

    pub async fn is_failed<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        is_failed(&mut **tx, message_id, now).await
    }

    pub async fn is_succeeded<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        is_succeeded(&mut **tx, message_id, now).await
    }

    pub async fn is_dead<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        message_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        is_dead(&mut **tx, message_id, now).await
    }
}
