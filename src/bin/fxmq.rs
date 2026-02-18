use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "fxmq")]
#[command(about = "Migrate fx-mq-building-blocks database schema")]
struct Args {
    /// Schema name to create and migrate
    #[arg(long)]
    schema_name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("DATABASE_URL environment variable not set"))?;

    info!("Connecting to database");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;

    info!("Running migrations for schema: {}", args.schema_name);
    fx_mq_building_blocks::migrator::run_migrations(&pool, &args.schema_name).await?;

    info!("Migrations completed successfully");

    Ok(())
}
