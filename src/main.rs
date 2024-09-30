use std::env;
use std::env::VarError;
use std::time::{SystemTime, UNIX_EPOCH};
use aws_lambda_events::event::cloudwatch_events::CloudWatchEvent;
use chrono::{DateTime, MappedLocalTime, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::{Tz, US};
use futures::{StreamExt, TryStreamExt};
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use lambda_runtime::tracing::{error, info, warn};
use serde::{Deserialize, Deserializer};
use serde::de::Error as DeserializerError;
use aws_sdk_dynamodb::Client as DynamoDbClient;

fn unix_timestamp_ms() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards: system time is somehow set to before the UNIX epoch.");
    i64::try_from(now.as_millis())
        .expect("Current UNIX timestamp is too big for i64. Hello from 2024, creature living in 7849!")
}

/// Describes a row of data as parsed from the CSV file
#[derive(Debug, Deserialize)]
struct WasteWaterCsvRow {
    #[serde(rename = "Sample Collection Date")]
    sample_collection_date: NaiveDate,
    #[serde(rename = "Site Name")]
    site_name: String,
    #[serde(rename = "County")]
    county: String,
    #[serde(rename = "PCR Pathogen Target")]
    pcr_pathogen_target: String,
    #[serde(rename = "PCR Gene Target")]
    pcr_gene_target: String,
    #[serde(rename = "Normalized Pathogen Concentration (gene copies/person/day)")]
    normalized_pathogen_concentration: f64,
    #[serde(rename = "Date/Time Updated")]
    #[serde(deserialize_with = "deserialize_pdt_datetime")]
    date_updated: DateTime<Tz>
}

fn deserialize_pdt_datetime<'de, D>(deserializer: D) -> Result<DateTime<Tz>, D::Error> where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    let mapped_date_time = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S.%f")
        .map_err(D::Error::custom)?
        .and_local_timezone(US::Pacific);

    match mapped_date_time {
        // 99% of cases
        MappedLocalTime::Single(date_time) => Ok(date_time),
        // Clock was turned backwards and now there are two times
        MappedLocalTime::Ambiguous(_, latest) => Ok(latest),
        // Clock was turned forwards and the time doesn't exit
        MappedLocalTime::None => Err(D::Error::custom(format!("Datetime {} is invalid for Pacific timezone.", s)))
    }
}

#[derive(Debug)]
struct WasteWaterSample {
    /// Semicolon separated concatenation of Site Name, County, PCR Pathogen Target, and the PCR Gene Target
    sample_summary: String,
    /// The sample collection date concatenated with the unix timestamp in ms this row was created.
    sample_collection_sort: String,
    sample_collection_date: NaiveDate,
    site_name: String,
    county: String,
    pcr_pathogen_target: String,
    pcr_gene_target: String,
    normalized_pathogen_concentration: f64,
    date_updated: DateTime<Tz>,
    poll_timestamp: i64
}

impl From<WasteWaterCsvRow> for WasteWaterSample {
    fn from(row: WasteWaterCsvRow) -> Self {
        let sample_summary = [
            row.site_name.as_str(),
            &row.county,
            &row.pcr_pathogen_target,
            &row.pcr_gene_target].join(";");

        let poll_timestamp = unix_timestamp_ms();

        let sample_collection_sort = format!(
            "{};{}",
            row.sample_collection_date.format("%Y-%m-%d"),
            poll_timestamp
        );

        Self {
            sample_summary,
            sample_collection_sort,
            sample_collection_date: row.sample_collection_date,
            site_name: row.site_name,
            county: row.county,
            pcr_pathogen_target: row.pcr_pathogen_target,
            pcr_gene_target: row.pcr_gene_target,
            normalized_pathogen_concentration: row.normalized_pathogen_concentration,
            date_updated: row.date_updated,
            poll_timestamp
        }
    }
}

async fn handler(wastewater_url: &str, event: LambdaEvent<CloudWatchEvent>) -> Result<(), Error> {
    let response = reqwest::get(wastewater_url).await?.error_for_status()?;
    let response_reader = response.bytes_stream().map_err(std::io::Error::other).into_async_read();
    let mut csv_reader = csv_async::AsyncDeserializer::from_reader(response_reader);
    let records = csv_reader.deserialize::<WasteWaterCsvRow>();
    let mut sample_data = records.map_ok(WasteWaterSample::from);

    while let Some(record_result) = sample_data.next().await {
        match record_result {
            Ok(record) => println!("{:?}", record),
            Err(e) => eprintln!("Error getting record: {:?}", e)
        }
    }

    Ok(())
}

static ENVVAR_WASTEWATER_URL: &str = "URL_WAGOV_WASTEWATER";
static DEFAULT_WASTEWATER_URL: &str = "https://doh.wa.gov/sites/default/files/Data/Downloadable_Wastewater.csv";

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    let wastewater_url = match env::var(ENVVAR_WASTEWATER_URL) {
        Ok(url) => {
            info!("Wastewater_url: {}", url);
            url
        }
        Err(VarError::NotPresent) => {
            warn!("{ENVVAR_WASTEWATER_URL} not set, using default: {DEFAULT_WASTEWATER_URL}");
            DEFAULT_WASTEWATER_URL.to_string()
        }
        Err(e) => panic!("Error getting {ENVVAR_WASTEWATER_URL}: {e}")
    };

    //let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    //let dynamodb_client_config = aws_sdk_dynamodb::config::Builder::from(&config).build();
    //let dynamodb_client = DynamoDbClient::from_conf(dynamodb_client_config);

    run(service_fn(|event: LambdaEvent<CloudWatchEvent>| async {
        handler(&wastewater_url, event).await
    })).await
}
