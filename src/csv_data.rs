use std::io::Read;

use chrono::{DateTime, MappedLocalTime, NaiveDate, NaiveDateTime};
use chrono_tz::{Tz, US};
use serde::{de::Error as DeserializerError, Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
/// Describes a row of data as parsed from the CSV file
pub struct WasteWaterCsvRow {
    #[serde(rename = "Sample Collection Date")]
    pub sample_collection_date: NaiveDate,
    #[serde(rename = "Site Name")]
    pub site_name: String,
    #[serde(rename = "County")]
    pub county: String,
    #[serde(rename = "PCR Pathogen Target")]
    pub pcr_pathogen_target: String,
    #[serde(rename = "PCR Gene Target")]
    pub pcr_gene_target: String,
    #[serde(rename = "Normalized Pathogen Concentration (gene copies/person/day)")]
    pub normalized_pathogen_concentration: f64,
    // Date the data was last updated.
    // This changes every time the data file is updated, but all rows have the same value.
    #[serde(rename = "Date/Time Updated")]
    #[serde(deserialize_with = "deserialize_pdt_datetime")]
    pub date_updated: DateTime<Tz>,
}

fn deserialize_pdt_datetime<'de, D>(deserializer: D) -> Result<DateTime<Tz>, D::Error>
where
    D: Deserializer<'de>,
{
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
        MappedLocalTime::None => Err(D::Error::custom(format!(
            "Datetime {} is invalid for Pacific timezone.",
            s
        ))),
    }
}

pub fn parse_data(reader: impl Read) -> impl Iterator<Item = csv::Result<WasteWaterCsvRow>> {
    let csv_reader = csv::Reader::from_reader(reader);
    csv_reader.into_deserialize()
}
