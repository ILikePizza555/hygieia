mod csv_data;
mod db;
mod useful;

use color_eyre::eyre::{self, Context};
use rusqlite::Connection;
use tracing::{debug, info, instrument, warn};

static ENVVAR_WASTEWATER_URL: &str = "URL_WAGOV_WASTEWATER";
static DEFAULT_WASTEWATER_URL: &str =
    "https://doh.wa.gov/sites/default/files/Data/Downloadable_Wastewater.csv";

fn get_wastewater_url() -> eyre::Result<String> {
    let wastewater_url = useful::env_or_else(ENVVAR_WASTEWATER_URL, || {
        info!("{ENVVAR_WASTEWATER_URL} not set, using default: {DEFAULT_WASTEWATER_URL}");
        DEFAULT_WASTEWATER_URL.to_string()
    })
    .with_context(|| format!("Error getting {ENVVAR_WASTEWATER_URL}"))?;

    Ok(wastewater_url)
}

static ENVVAR_SQLITE_DB_PATH: &str = "SQLITE_DB_PATH";
static DEFAULT_SQLITE_DB_PATH: &str = "wastewater.sqlite";

fn get_sqlite_db_path() -> eyre::Result<String> {
    let sqlite_db_path = useful::env_or_else(ENVVAR_SQLITE_DB_PATH, || {
        info!("{ENVVAR_SQLITE_DB_PATH} not set, using default: {DEFAULT_SQLITE_DB_PATH}");
        DEFAULT_SQLITE_DB_PATH.to_string()
    })
    .with_context(|| format!("Error getting {ENVVAR_SQLITE_DB_PATH}"))?;

    Ok(sqlite_db_path)
}

/// Opens a connection to the SQLite database, creating it if it doesn't exist.
/// Applies schema if it doesn't exist.
fn init_sqlite_db() -> eyre::Result<Connection> {
    let sqlite_db_path = get_sqlite_db_path()?;
    debug!("Opening SQLite DB at {sqlite_db_path}");

    let db_conn = Connection::open(sqlite_db_path)?;
    debug!("Successfully opened SQLite DB.");

    // Apply schema
    db_conn.execute_batch(include_str!("schema.sql"))?;

    Ok(db_conn)
}

#[instrument]
fn init() -> eyre::Result<(String, Connection)> {
    // Load environment variables
    // Want to do it before init_tracing to load rust_log
    dotenvy::dotenv()?;

    useful::init_tracing();

    // Load Wastewater URL from environment variable, defaulting to DEFAULT_WASTEWATER_URL if not set
    let wastewater_url = get_wastewater_url()?;
    debug!("Loaded Wastewater URL from ENV: {}", wastewater_url);

    // Load sqlite database, creating it if it doesn't exist
    let db_conn = init_sqlite_db()?;

    Ok((wastewater_url, db_conn))
}

fn main() -> eyre::Result<()> {
    let (wastewater_url, mut db_conn) = init()?;

    info!("Requesting Wastewater data from {}", wastewater_url);

    let response = ureq::get(&wastewater_url).call()?;
    info!(
        "Response: OK, Content-Type: {:?}, Content-Length: {:?}",
        response.header("Content-Type"),
        response.header("Content-Length")
    );
    let reader = response.into_reader();

    let data = csv_data::parse_data(reader).filter_map(|r| r.ok());
    db::insert_wastewater_samples(&mut db_conn, data)?;

    Ok(())
}
