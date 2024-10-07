mod csv_data;
mod db;
mod useful;

use std::env;

use color_eyre::eyre::{self, Context};
use rusqlite::{params, Connection};
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

static ENVVAR_SQLITE_DB_PATH: &str = "PATH_SQLITE_DB";
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

static ENVVAR_DISCORD_WEBHOOK_URL: &str = "URL_DISCORD_WEBHOOK";
fn get_discord_webhook() -> eyre::Result<String> {
    env::var(ENVVAR_DISCORD_WEBHOOK_URL)
        .with_context(|| format!("Error getting {ENVVAR_DISCORD_WEBHOOK_URL}"))
}

#[instrument]
fn init() -> eyre::Result<(String, String, Connection)> {
    // Load environment variables
    // Want to do it before init_tracing to load rust_log
    dotenvy::dotenv()?;

    useful::init_tracing();

    // Load Wastewater URL from environment variable, defaulting to DEFAULT_WASTEWATER_URL if not set
    let wastewater_url = get_wastewater_url()?;
    debug!("Loaded Wastewater URL from ENV: {}", wastewater_url);

    // Load sqlite database, creating it if it doesn't exist
    let db_conn = init_sqlite_db()?;

    Ok((wastewater_url, get_discord_webhook()?, db_conn))
}

fn main() -> eyre::Result<()> {
    let (wastewater_url, discord_webook, mut db_conn) = init()?;

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

    // Query the database for latest samples and differences in Pierce and King counties
    let counties = ["Pierce", "King"];
    let variants = ["FLUAV", "FLUBV", "RSV", "sars-cov-2"];

    let query = r#"
        WITH ranked_samples AS (
            SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY pcr_pathogen_target ORDER BY sample_collection_date DESC) as row_num
            FROM wastewater_samples
            WHERE county = ?1 AND pcr_pathogen_target = ?2
        )
        SELECT 
            s1.normalized_pathogen_concentration as latest_value,
            s1.sample_collection_date as latest_date,
            s1.normalized_pathogen_concentration - s2.normalized_pathogen_concentration as difference,
            s2.sample_collection_date as previous_date
        FROM ranked_samples s1
        LEFT JOIN ranked_samples s2 ON s2.row_num = 2 AND s1.pcr_pathogen_target = s2.pcr_pathogen_target
        WHERE s1.row_num = 1
    "#;

    let data: Vec<(
        String,
        String,
        rusqlite::Result<(f64, String, Option<f64>, Option<String>)>,
    )> = counties
        .iter()
        .flat_map(|&county| variants.map(|variant| (county, variant)))
        .map(|(county, variant)| {
            (
                county.to_owned(),
                variant.to_owned(),
                db_conn.query_row(query, params![county, variant], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                }),
            )
        })
        .collect();

    let mut content_vec = vec![
        "Hello World! I've gathered the latest respratory illness wastewater data:".to_owned(),
    ];

    for result in data {
        match result {
            (county, variant, Ok((latest_value, latest_date, difference, previous_date))) => {
                info!(
                    "{} County - {}: Latest value: {} on {}, Difference: {:?} (Previous date: {:?})",
                    county, variant, latest_value, latest_date, difference, previous_date
                );

                content_vec.push(format!("**{county} County - {variant}**: {latest_value} ({difference:?}) on {latest_date}"));
            }
            (county, variant, Err(e)) => {
                warn!("No data found for {} County - {}: {}", county, variant, e);

                content_vec.push(format!("**{county} County - {variant}**: There was an error getting data for this. Yell at Izzy."));
            }
        }
    }

    let message = content_vec.join("\n");
    let discord_webhook_response =
        ureq::post(&discord_webook).send_form(&[("content", &message)])?;

    info!(
        "Response: OK, Content-Type: {:?}, Content-Length: {:?}",
        discord_webhook_response.header("Content-Type"),
        discord_webhook_response.header("Content-Length")
    );

    Ok(())
}
