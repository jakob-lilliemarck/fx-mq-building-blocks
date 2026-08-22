# Design: PollFilter for Poll Queries

## Motivation

The `fx-mq-jobs` worker-free listener design needs to exclude messages from
saturated concurrency groups at the database query level. Different listener
instances have different groups saturated, so the client must tell the
database which message names to exclude from poll results.

## Design

### `Filter` struct

```rust
/// Filter criteria for poll queries.
///
/// Used to exclude specific message names from poll results at the
/// database query level. Default (empty) applies no filtering.
///
/// # Example
///
/// ```ignore
/// let filter = Filter::default()
///     .with_exclude_names(vec!["heavy-job".into()]);
/// let msg = get_next_unattempted_with_filter(
///     &mut tx, now, host_id, hold_for, filter
/// ).await?;
/// ```
#[derive(Debug, Default, Clone)]
pub struct Filter {
    /// Message names to exclude from poll results.
    /// When empty, no filtering is applied.
    pub exclude_names: Vec<String>,
}

impl Filter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_exclude_names(mut self, names: Vec<String>) -> Self {
        self.exclude_names = names;
        self
    }
}
```

### Query pattern

Each of the three poll query files follows the same pattern:

1. The existing function is marked `#[deprecated]` and becomes a thin
   wrapper delegating to the new `_with_filter` variant with
   `Filter::default()` (empty filter = no filtering = identical behavior).
2. A new `_with_filter` function contains the actual SQL implementation,
   adding `WHERE name != ALL($4)` to the innermost message SELECT.

### SQL change per query

#### `get_next_unattempted`

```sql
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
-- leased, attempted CTEs unchanged
...
```

`$4` is `filter.exclude_names: Vec<String>`, mapped to `TEXT[]` by sqlx.
When the array is empty, `!= ALL('{}'::text[])` is vacuously true — no
filtering, same results as the unfiltered query.

#### `get_next_retryable`

The `attempts_failed` table does not have a `name` column. Filter via
`EXISTS` subquery on `messages_attempted`:

```sql
WITH next_retryable AS (
    SELECT fa.message_id, fa.attempted
    FROM attempts_failed fa
    WHERE fa.retry_earliest_at <= $1
      AND NOT EXISTS (
          SELECT 1 FROM leases l
          WHERE l.message_id = fa.message_id AND l.expires_at > $1
      )
      AND EXISTS (
          SELECT 1 FROM messages_attempted ma
          WHERE ma.id = fa.message_id
            AND ma.name != ALL($4)
      )
      AND fa.failed_at = (
          SELECT MAX(fa2.failed_at)
          FROM attempts_failed fa2
          WHERE fa2.message_id = fa.message_id
      )
    ORDER BY fa.failed_at ASC, fa.message_id ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
-- leased, final SELECT unchanged
...
```

#### `get_next_missing`

```sql
WITH candidate AS (
    SELECT ma.*
    FROM leases l
    JOIN messages_attempted ma ON ma.id = l.message_id
    WHERE l.expires_at < $1
      AND NOT EXISTS (...succeeded...)
      AND NOT EXISTS (...dead...)
      AND ma.name != ALL($4)
    ORDER BY ma.published_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
...
```

### Function signatures

#### Free functions (in each query file)

```rust
// Deprecated — delegates to _with_filter with Filter::default()
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
        r#"...
        WHERE name != ALL($4)
        ..."#,
        now,
        host_id,
        expires_at,
        exclude_names as &[String],
    )
    .fetch_optional(tx)
    .await?;

    Ok(message)
}
```

Same pattern for `get_next_retryable` → `get_next_retryable_with_filter`
and `get_next_missing` → `get_next_missing_with_filter`, each with their
own doc comment and deprecation notice containing the full signature.

### `Queries` wrapper (`with_schema.rs`)

```rust
use crate::queries::Filter;

impl Queries {
    // ── Deprecated wrappers ──────────────────────────────────────────

    #[deprecated = "use get_next_unattempted_with_filter(tx, now, host_id, hold_for, Filter::default())"]
    pub async fn get_next_unattempted<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        now: DateTime<Utc>,
        host_id: Uuid,
        hold_for: Duration,
    ) -> Result<Option<RawMessage>, sqlx::Error> {
        self.get_next_unattempted_with_filter(tx, now, host_id, hold_for, Filter::default())
            .await
    }

    // (same pattern for get_next_retryable, get_next_missing)

    // ── New methods ──────────────────────────────────────────────────

    /// Polls for the next unattempted message within `self.schema`,
    /// optionally filtering out specified message names.
    pub async fn get_next_unattempted_with_filter<'tx>(
        &self,
        tx: &mut PgTransaction<'tx>,
        now: DateTime<Utc>,
        host_id: Uuid,
        hold_for: Duration,
        filter: Filter,
    ) -> Result<Option<RawMessage>, sqlx::Error> {
        set_schema_for_transaction(tx, &self.schema).await?;
        get_next_unattempted_with_filter(&mut **tx, now, host_id, hold_for, filter).await
    }

    // (same pattern for get_next_retryable_with_filter,
    //  get_next_missing_with_filter)
}
```

## Files affected

| File | Action |
|------|--------|
| `src/queries/filter.rs` | **NEW** — `Filter` struct + `Debug`, `Default`, `Clone` impls + `new()`, `with_exclude_names()` |
| `src/queries/get_next_unattempted.rs` | Add `#[deprecated]` on existing fn, add `get_next_unattempted_with_filter()` |
| `src/queries/get_next_retryable.rs` | Add `#[deprecated]` on existing fn, add `get_next_retryable_with_filter()` |
| `src/queries/get_next_missing.rs` | Add `#[deprecated]` on existing fn, add `get_next_missing_with_filter()` |
| `src/queries/mod.rs` | Add `mod filter;`, add `pub use filter::Filter;` |
| `src/queries/with_schema.rs` | Add `#[deprecated]` on existing `Queries` methods, add `_with_filter` methods |
| `src/lib.rs` | Optionally add `pub use queries::Filter;` for top-level re-export |

No files deleted. No existing function signatures changed — only new variants
added and old ones deprecated with delegation.

## Tests

Existing tests are updated to use the `_with_filter` variants with
`Filter::default()`. The test logic itself does not change — only the
function call signatures. New tests added for each `_with_filter` variant:

| Test | Scenario |
|------|----------|
| `it_filters_out_excluded_message_names` | Publish two messages with different names, pass filter excluding one name, verify only the non-excluded message is returned |
| `it_returns_all_when_exclude_is_empty` | Publish messages, pass `Filter::default()`, verify all returned (same behavior as deprecated fn) |
| `it_returns_none_when_all_excluded` | Publish one message, exclude its name, verify `None` |
| `it_skips_locked_with_filter` | Two transactions, one holds a lock, other with filter skips locked row |

For `get_next_retryable_with_filter`: tests first move a message to retryable
state (via unattempted + report_retryable), then poll with filter.

For `get_next_missing_with_filter`: tests use short lease, wait for expiry,
then poll with filter.

## Cargo.toml

Bump version from `0.2.0` to `0.3.0` (new public API: `Filter` type and
`_with_filter` query methods).

## Backward compatibility

- All existing public functions and methods remain available, marked
  `#[deprecated]` but fully functional.
- Existing callers get compiler warnings nudging them toward the new API.
- The `Filter::default()` produces identical results to the unfiltered
  queries.
- No schema changes. No behavior changes for existing callers.
