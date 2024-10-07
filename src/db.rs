use chrono::{DateTime, NaiveDate};
use chrono_tz::Tz;

use crate::{
    csv_data::WasteWaterCsvRow,
    useful::{self, try_unix_timestamp},
};

#[derive(Debug)]
/// A normalized record of a wastewater sample.
/// The "primay key" of this value is the combination of sample_collection_date, site_name, county, pcr_pathogen_target, and pcr_gene_target.
struct WasteWaterSample {
    /// Date the sample was collected, but not when the data was polled.
    sample_collection_date: NaiveDate,
    /// Name of the site where the sample was collected.
    site_name: String,
    /// County where the sample was collected.
    county: String,
    /// Pathogen target for the PCR test.
    pcr_pathogen_target: String,
    /// Gene target for the PCR test.
    pcr_gene_target: String,
    /// Normalized pathogen concentration (gene copies/person/day).
    /// Note that each site uses a different normalization method, so this value is not comparable between sites.
    normalized_pathogen_concentration: f64,
    /// Date the data was last updated.
    date_updated: DateTime<Tz>,
    // Unix timestamp of when this data was polled and added to the database.
    poll_timestamp: u64,
}

impl From<WasteWaterCsvRow> for WasteWaterSample {
    fn from(row: WasteWaterCsvRow) -> Self {
        Self {
            sample_collection_date: row.sample_collection_date,
            site_name: row.site_name,
            county: row.county,
            pcr_pathogen_target: row.pcr_pathogen_target,
            pcr_gene_target: row.pcr_gene_target,
            normalized_pathogen_concentration: row.normalized_pathogen_concentration,
            date_updated: row.date_updated,
            poll_timestamp: try_unix_timestamp().unwrap(),
        }
    }
}
