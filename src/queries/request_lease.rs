use chrono::{DateTime, Utc};
use sqlx::PgExecutor;
use std::time::Duration;
use uuid::Uuid;

/// Requests a lease on a message
/// Will only acquire if no other host holds currently hold a lease for the requested message
/// Returns None if no lease could be acquired, otherwise Some(expires_at)
pub async fn request_lease<'tx, E: PgExecutor<'tx>>(
    tx: E,
    message_id: Uuid,
    now: DateTime<Utc>,
    host_id: Uuid,
    hold_for: Duration,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    let expires_at = sqlx::query_scalar!(
        r#"
        INSERT INTO leases (
            message_id,
            acquired_at,
            acquired_by,
            expires_at
        )
        SELECT
            $1, $2, $3, $4
        WHERE not exists (
            SELECT *
            FROM leases
            WHERE acquired_by != $3 AND expires_at > $2
        )
        RETURNING expires_at;
        "#,
        message_id,
        now,
        host_id,
        now + hold_for
    )
    .fetch_optional(tx)
    .await?;

    Ok(expires_at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{queries::request_lease, testing_tools::has_active_lease};
    use chrono::SubsecRound;
    use std::time::Duration;
    use uuid::Uuid;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_acquires_a_lease_when_none_is_held(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let message_id = Uuid::now_v7();
        let host_id = Uuid::now_v7();
        let now = Utc::now();
        let hold_for = Duration::from_mins(1);

        let actual =
            request_lease(&pool, message_id, now, host_id, hold_for).await?;

        assert_eq!(Some((now + hold_for).trunc_subsecs(6)), actual);
        assert!(has_active_lease(&pool, message_id, now).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_acquires_a_lease_when_one_id_held_by_the_requesting_host(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let message_id = Uuid::now_v7();
        let host_id = Uuid::now_v7();
        let now = Utc::now();
        let hold_for = Duration::from_mins(1);

        let actual =
            request_lease(&pool, message_id, now, host_id, hold_for).await?;

        assert_eq!(Some((now + hold_for).trunc_subsecs(6)), actual);
        assert!(has_active_lease(&pool, message_id, now).await?);

        // Sleep for a period long enough to ensure the postgres timestamp will be different
        tokio::time::sleep(Duration::from_micros(1)).await;

        let now = Utc::now();

        let actual =
            request_lease(&pool, message_id, now, host_id, hold_for).await?;

        assert_eq!(Some((now + hold_for).trunc_subsecs(6)), actual);
        assert!(has_active_lease(&pool, message_id, now).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_acquires_a_lease_when_there_is_an_expired_lease_from_another_host(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let message_id = Uuid::now_v7();
        let host_id = Uuid::now_v7();
        let now = Utc::now();
        let hold_for = Duration::from_millis(10);

        let actual =
            request_lease(&pool, message_id, now, host_id, hold_for).await?;

        assert_eq!(Some((now + hold_for).trunc_subsecs(6)), actual);
        assert!(has_active_lease(&pool, message_id, now).await?);

        // wait for the current lease to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        let host_id = Uuid::now_v7();
        let now = Utc::now();

        let actual =
            request_lease(&pool, message_id, now, host_id, hold_for).await?;

        assert_eq!(Some((now + hold_for).trunc_subsecs(6)), actual);
        assert!(has_active_lease(&pool, message_id, now).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_does_not_acquire_a_lease_when_one_is_held_by_another_host(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let message_id = Uuid::now_v7();
        let host_id = Uuid::now_v7();
        let now = Utc::now();
        let hold_for = Duration::from_mins(1);

        let actual =
            request_lease(&pool, message_id, now, host_id, hold_for).await?;

        assert_eq!(Some((now + hold_for).trunc_subsecs(6)), actual);
        assert!(has_active_lease(&pool, message_id, now).await?);

        let host_id = Uuid::now_v7();
        let now = Utc::now();

        let actual =
            request_lease(&pool, message_id, now, host_id, hold_for).await?;

        assert_eq!(None, actual);

        Ok(())
    }
}
