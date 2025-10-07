use sqlx::{Acquire, Postgres};

#[derive(Debug)]
pub struct PgIdentifier {
    ident: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PgIdentifierParsingError {
    #[error("Identifiers may not be empty")]
    Empty,
    #[error("Identifiers may not exceed 63 bytes")]
    TooLarge,
    #[error("Identifiers must start with [A-Za-z] or \"_\"")]
    InvalidFirstChar,
}

const DEFAULT_NAMEDATALEN: usize = 64; // Postgres default, compile time configured constant

impl PgIdentifier {
    pub fn parse(raw: &str) -> Result<Self, PgIdentifierParsingError> {
        if raw.is_empty() {
            return Err(PgIdentifierParsingError::Empty);
        }

        if raw.len() > DEFAULT_NAMEDATALEN - 1 {
            return Err(PgIdentifierParsingError::TooLarge);
        }

        let first_char = raw.chars().next().unwrap(); // safe after empty check
        if !first_char.is_ascii_alphabetic() && first_char != '_' {
            return Err(PgIdentifierParsingError::InvalidFirstChar);
        }

        // Escape internal double quotes
        let escaped = raw.replace('"', "\"\"");

        // Wrap in quotes for PostgreSQL
        Ok(PgIdentifier {
            ident: format!("\"{}\"", escaped),
        })
    }

    pub fn as_ref(&self) -> &str {
        &self.ident
    }
}

impl std::fmt::Display for PgIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.ident)
    }
}

#[cfg(test)]
mod tests {
    use crate::migrator::PgIdentifier;

    #[test]
    fn it_escapes_wierd_identifier_names() -> anyhow::Result<()> {
        let ident = PgIdentifier::parse("test\"test")?;
        assert_eq!(ident.as_ref(), "\"test\"\"test\"");

        Ok(())
    }

    #[test]
    fn it_errors_on_invalid_empty_identifier() -> anyhow::Result<()> {
        let ident = PgIdentifier::parse("");
        assert!(ident.is_err());
        Ok(())
    }

    #[test]
    fn it_rejects_too_large_identifier_names() -> anyhow::Result<()> {
        assert!(PgIdentifier::parse("🐍🐍🐍🐍🐍🐍🐍🐍🐍🐍🐍🐍🐍🐍🐍🐍").is_err());
        Ok(())
    }

    #[test]
    fn it_rejects_invalid_first_chars() -> anyhow::Result<()> {
        assert!(PgIdentifier::parse("$").is_err());
        assert!(PgIdentifier::parse("(").is_err());
        assert!(PgIdentifier::parse(")").is_err());
        assert!(PgIdentifier::parse("[").is_err());
        assert!(PgIdentifier::parse("]").is_err());
        assert!(PgIdentifier::parse(",").is_err());
        assert!(PgIdentifier::parse(";").is_err());
        assert!(PgIdentifier::parse(":").is_err());
        assert!(PgIdentifier::parse("*").is_err());
        assert!(PgIdentifier::parse(".").is_err());
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MigratorError {
    #[error("InvalidIdentifierError: {0}")]
    InvalidIdentifier(#[from] PgIdentifierParsingError),
    #[error("DatabaseError: {0}")]
    Database(#[from] sqlx::Error),
    #[error("MigrateError: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

// Embed the migrations directory at compile time
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// Runs database migrations for the fx-event-bus schema.
///
/// Creates the 'fx_event_bus' schema if it doesn't exist and runs all
/// embedded migrations within that schema.
///
/// # Arguments
///
/// * `conn` - Database connection or connection pool
///
/// # Errors
///
/// Returns `sqlx::Error` if schema creation or migration execution fails.
pub async fn run_migrations<'a, A>(conn: A, schema: &str) -> Result<(), MigratorError>
where
    A: Acquire<'a, Database = Postgres>,
{
    let schema_ident = PgIdentifier::parse(schema)?;

    let mut tx = conn.begin().await?;

    // Ensure the schema exists
    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {};", schema_ident.as_ref());
    sqlx::query(&create_schema).execute(&mut *tx).await?;

    // Temporarily set search_path for this transaction
    let set_search_path = format!("SET LOCAL search_path TO {};", schema_ident.as_ref());
    sqlx::query(&set_search_path).execute(&mut *tx).await?;

    // Run migrations within the schema
    MIGRATOR.run(&mut *tx).await?;

    tx.commit().await?;

    Ok(())
}
