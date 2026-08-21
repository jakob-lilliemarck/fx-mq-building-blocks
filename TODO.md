# TODO: `get_next_missing` is incorrect under lease renewal

## Context

`fx_mq_jobs::LeaseRenewer::renew_lease` (which calls `request_lease`) INSERTs a
new `leases` row on every renewal. The table is append-only in that sense:
`PRIMARY KEY (message_id, expires_at)`, no uniqueness on `message_id`, and the
"current" lease is the row with the latest `expires_at`. So a message that is
being renewed accumulates multiple lease rows — stale expired ones plus the
active one.

The canonical state model lives in `src/testing_tools.rs` (`State::get_state`):

- `InProgress` = attempted + `has_any_lease` + `has_active_lease` (`expires_at > now`)
- `Missing`    = attempted + `has_any_lease` + **NOT** `has_active_lease`

## Bug 1 — a stale row marks a live message as "missing"

`src/queries/get_next_missing.rs` selects a message when **any** lease row is
expired:

```sql
FROM leases l
JOIN messages_attempted ma ON ma.id = l.message_id
WHERE l.expires_at < $1
```

This does not match `State::Missing` (which requires that **no** lease is still
active). With multiple lease rows, a message whose latest lease is still active
gets selected as "missing" because an older row has expired, so a second worker
can re-acquire a message that is actively being processed.

`get_next_retryable` already does this correctly:

```sql
AND NOT EXISTS (
    SELECT 1 FROM leases l
    WHERE l.message_id = fa.message_id AND l.expires_at > $1
)
```

## Bug 2 — the re-acquisition UPDATE collides on the primary key

`get_next_missing` re-acquires via:

```sql
UPDATE leases le
SET acquired_at = $1, acquired_by = $2, expires_at = $3
FROM candidate c
WHERE le.message_id = c.id
```

This rewrites **all** of a message's lease rows to the same `expires_at`,
colliding on `PRIMARY KEY (message_id, expires_at)` whenever the message has
two or more rows. The other readers (`get_next_unattempted`,
`get_next_retryable`) INSERT a fresh row instead.

## Proposed fix

Match `State::Missing` in the candidate (has a lease, none active) and
re-acquire by INSERTing a fresh lease row:

```sql
WITH candidate AS (
    SELECT ma.*
    FROM messages_attempted ma
    WHERE NOT EXISTS (SELECT 1 FROM attempts_succeeded s WHERE s.message_id = ma.id)
      AND NOT EXISTS (SELECT 1 FROM attempts_dead d WHERE d.message_id = ma.id)
      AND EXISTS (SELECT 1 FROM leases l WHERE l.message_id = ma.id)
      AND NOT EXISTS (
          SELECT 1 FROM leases l2
          WHERE l2.message_id = ma.id AND l2.expires_at > $1
      )
    ORDER BY ma.published_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
leased AS (
    INSERT INTO leases (message_id, acquired_at, acquired_by, expires_at)
    SELECT c.id, $1, $2, $3
    FROM candidate c
)
SELECT c.id, c.name, c.hash, c.payload, 0 "attempted!"
FROM candidate c;
```

## Tests to add

- A message with one expired + one **active** lease row is **not** returned.
- A message with multiple **expired** lease rows is returned and re-acquired
  **without** a primary-key violation.
- The existing single-row test (`it_gets_the_next_missing_message`) still passes.

## Related note

`get_next_missing` hardcodes `0 "attempted!"` (a crash does not count as an
attempt), unlike `get_next_retryable` which preserves the counter. Likely
intentional; confirm while here.
