use crate::models::{Message, RawMessage};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgExecutor;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
enum State {
    Pending,    // unattempted only
    InProgress, // attempted only with an active lease
    Missing,    // attempted only with an expired lease
    Failed,     // attempted and failed without any lease
    Succeeded,  // attempted and succeeded without any lease
    Dead,       // attempted and dead without any lease
}

#[derive(Debug)]
struct RawState {
    pub is_pending: bool,
    pub is_attempted: bool,
    pub has_any_lease: bool,
    pub has_active_lease: bool,
    pub has_failed_attempts: bool,
    pub has_errors: bool,
    pub is_succeeded: bool,
    pub is_dead: bool,
}

impl RawState {
    fn get_state(&self) -> State {
        match (
            self.is_pending,
            self.is_attempted,
            self.has_any_lease,
            self.has_active_lease,
            self.has_failed_attempts,
            self.has_errors,
            self.is_succeeded,
            self.is_dead,
        ) {
            // UNATTEMPTED: unattempted only
            (
                true,  // is_unattempted
                false, // is_attempted
                false, // has_any_lease
                false, // has_active_lease
                false, // has_failed_attempts
                false, // has_errors
                false, // is_succeeded
                false, // is_dead
            ) => State::Pending,
            // IN PROGRESS: attempted, maybe failed with errors and with an _active_ lease
            (
                false, // is_unattempted
                true,  // is_attempted
                true,  // has_any_lease
                true,  // has_active_lease
                _,     // has_failed_attempts
                _,     // has_errors
                false, // is_succeeded
                false, // is_dead
            ) => State::InProgress,
            // MISSING: attempted, maybe failed with errors and with an expired lease
            (
                false, // is_unattempted
                true,  // is_attempted
                true,  // has_any_lease
                false, // has_active_lease
                _,     // has_failed_attempts
                _,     // has_errors
                false, // is_succeeded
                false, // is_dead
            ) => State::Missing,
            // FAILED: attempted and failed with errors without any lease
            (
                false, // is_unattempted
                true,  // is_attempted
                false, // has_any_lease
                false, // has_active_lease
                true,  // has_failed_attempts
                true,  // has_errors
                false, // is_succeeded
                false, // is_dead
            ) => State::Failed,
            // SUCCEEDED: attempted and succeeded, maybe with errors without any lease
            (
                false, // is_unattempted
                true,  // is_attempted
                false, // has_any_lease
                false, // has_active_lease
                false, // has_failed_attempts
                _,     // has_errors
                true,  // is_succeeded
                false, // is_dead
            ) => State::Succeeded,
            // DEAD: attempted and dead with errors without any lease
            (
                false, // is_unattempted
                true,  // is_attempted
                false, // has_any_lease
                false, // has_active_lease
                false, // has_failed_attempts
                true,  // has_errors
                false, // is_succeeded
                true,  // is_dead
            ) => State::Dead,
            _ => {
                panic!("Undefined state {:?}", self)
            }
        }
    }
}

async fn get_raw_state<'c, E: PgExecutor<'c>>(
    executor: E,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<RawState, sqlx::Error> {
    sqlx::query_as!(
        RawState,
        r#"
        WITH message AS (
            SELECT id
            FROM messages_unattempted
            WHERE id = $1

            UNION ALL

            SELECT id
            FROM messages_attempted
            WHERE id = $1
        ),
        attempts AS (
            SELECT *
            FROM attempts_failed af
            WHERE af.message_id = $1
        ),
        leases AS (
            SELECT *
            FROM leases l
            WHERE l.message_id = $1
        ),
        succeeded AS (
            SELECT *
            FROM attempts_succeeded s
            WHERE s.message_id = $1
        ),
        dead AS (
            SELECT *
            FROM attempts_dead d
            WHERE d.message_id = $1
        ),
        errors AS (
            SELECT *
            FROM errors e
            WHERE e.message_id = $1
        )
        SELECT
            -- True if message exists in unattempted table
            EXISTS (SELECT 1 FROM messages_unattempted mu WHERE mu.id = $1) AS "is_pending!",

            -- True if message exists in attempted table
            EXISTS (SELECT 1 FROM messages_attempted ma WHERE ma.id = $1) AS "is_attempted!",

            -- True if there are any leases (expired or active)
            EXISTS (SELECT 1 FROM leases) AS "has_any_lease!",

            -- True if there is a lease that has not expired yet
            EXISTS (SELECT 1 FROM leases WHERE expires_at > $2) AS "has_active_lease!",

            -- True if there are failed attempts
            EXISTS (SELECT 1 FROM attempts) AS "has_failed_attempts!",

            -- True if succeeded
            EXISTS (SELECT 1 FROM succeeded) AS "is_succeeded!",

            -- True if dead
            EXISTS (SELECT 1 FROM dead) AS "is_dead!",

            -- True if there are any errors
            EXISTS (SELECT 1 FROM errors) AS "has_errors!";

        "#,
        message_id,
        now
    )
    .fetch_one(executor)
    .await
}

async fn is_of_state<'c, E: PgExecutor<'c>>(
    executor: E,
    state: State,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let raw = get_raw_state(executor, message_id, now).await?;
    Ok(state == raw.get_state())
}

pub async fn is_pending<'c, E: PgExecutor<'c>>(
    executor: E,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    is_of_state(executor, State::Pending, message_id, now).await
}

pub async fn is_in_progress<'c, E: PgExecutor<'c>>(
    executor: E,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    is_of_state(executor, State::InProgress, message_id, now).await
}

pub async fn is_missing<'c, E: PgExecutor<'c>>(
    executor: E,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    is_of_state(executor, State::Missing, message_id, now).await
}

pub async fn is_failed<'c, E: PgExecutor<'c>>(
    executor: E,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    is_of_state(executor, State::Failed, message_id, now).await
}

pub async fn is_succeeded<'c, E: PgExecutor<'c>>(
    executor: E,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    is_of_state(executor, State::Succeeded, message_id, now).await
}

pub async fn is_dead<'c, E: PgExecutor<'c>>(
    executor: E,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    is_of_state(executor, State::Dead, message_id, now).await
}

pub async fn has_active_lease(
    pool: &sqlx::PgPool,
    message_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM leases
            WHERE message_id = $1 AND expires_at > $2
        )
        "#,
        message_id,
        now
    )
    .fetch_one(pool)
    .await?;

    Ok(exists.unwrap_or(false))
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TestMessage {
    pub message: String,
    pub value: i32,
}

impl TestMessage {
    pub fn new(
        message: String,
        value: i32,
    ) -> Self {
        Self { message, value }
    }

    pub fn to_raw(&self) -> anyhow::Result<RawMessage> {
        let payload = serde_json::to_value(&self)?;

        Ok(RawMessage {
            id: Uuid::now_v7(),
            name: TestMessage::NAME.to_string(),
            hash: TestMessage::HASH,
            payload: payload,
            attempted: 0,
        })
    }
}

impl Message for TestMessage {
    const NAME: &str = "TestMessage";
}

impl Default for TestMessage {
    fn default() -> Self {
        TestMessage {
            message: "whats the meaning of life, the universe and everything?"
                .to_string(),
            value: 42,
        }
    }
}
