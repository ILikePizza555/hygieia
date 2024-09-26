use std::env;
use aws_lambda_events::event::cloudwatch_events::CloudWatchEvent;
use chrono::{DateTime, Local, MappedLocalTime, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::{Tz, US};
use futures::{StreamExt, TryStreamExt};
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use lambda_runtime::tracing::info;
use serde::{Deserialize, Deserializer};
use serde::de::Error as DeserializerError;

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
        MappedLocalTime::None => Err(D::Error::custom(format!("Datetime {} is invalid for Pacific timezone", s)))
    }
}

async fn handler(wastewater_url: &str, event: LambdaEvent<CloudWatchEvent>) -> Result<(), Error> {
    let response = reqwest::get(wastewater_url).await?.error_for_status()?;
    let response_reader = response.bytes_stream().map_err(std::io::Error::other).into_async_read();
    let mut csv_reader = csv_async::AsyncDeserializer::from_reader(response_reader);
    let mut records = csv_reader.deserialize::<WasteWaterCsvRow>();

    


    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    let wastewater_url = env::var("URL_WAGOV_WASTEWATER").expect("No wastewater url environment variable.");

    info!(wastewater_url);

    run(service_fn(|event: LambdaEvent<CloudWatchEvent>| async {
        handler(&wastewater_url, event).await
    })).await
}
