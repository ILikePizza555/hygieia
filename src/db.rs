use std::{error::Error, time::SystemTimeError};

use chrono::{DateTime, FixedOffset, NaiveDate};
use color_eyre::eyre;
use rusqlite::{named_params, Connection, OptionalExtension, Row};
use tracing::{error, info, instrument, trace};

use crate::{csv_data::WasteWaterCsvRow, useful::try_unix_timestamp};

#[derive(Debug)]
/// A normalized record of a wastewater sample.
/// The "primay key" of this value is the combination of sample_collection_date, site_name, county, pcr_pathogen_target, and pcr_gene_target.
pub struct WasteWaterSample {
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
    date_updated: DateTime<FixedOffset>,
    // Unix timestamp of when this data was polled and added to the database.
    poll_timestamp: u64,
}

impl WasteWaterSample {
    fn from_row(row: &Row) -> Result<Self, rusqlite::Error> {
        Ok(Self {
            sample_collection_date: row.get(0)?,
            site_name: row.get(1)?,
            county: row.get(2)?,
            pcr_pathogen_target: row.get(3)?,
            pcr_gene_target: row.get(4)?,
            normalized_pathogen_concentration: row.get(5)?,
            date_updated: row.get(6)?,
            poll_timestamp: row.get(7)?,
        })
    }
}

impl TryFrom<WasteWaterCsvRow> for WasteWaterSample {
    type Error = SystemTimeError;

    fn try_from(row: WasteWaterCsvRow) -> Result<Self, Self::Error> {
        let poll_timestamp = try_unix_timestamp()?;

        Ok(Self {
            sample_collection_date: row.sample_collection_date,
            site_name: row.site_name,
            county: row.county,
            pcr_pathogen_target: row.pcr_pathogen_target,
            pcr_gene_target: row.pcr_gene_target,
            normalized_pathogen_concentration: row.normalized_pathogen_concentration,
            date_updated: row.date_updated.fixed_offset(),
            poll_timestamp,
        })
    }
}

/// Inserts a sample into the database if it doesn't exist.
/// Returns true if the sample was inserted, false otherwise.
pub fn insert_wastewater_sample(conn: &Connection, sample: WasteWaterSample) -> eyre::Result<bool> {
    const SELECT_SAMPLE_SQL: &str = "
    SELECT * FROM wastewater_samples
    WHERE sample_collection_date = :sample_collection_date
    AND site_name = :site_name
    AND county = :county
    AND pcr_pathogen_target = :pcr_pathogen_target
    AND pcr_gene_target = :pcr_gene_target";
    let mut select_stmt = conn.prepare_cached(SELECT_SAMPLE_SQL)?;

    const INSERT_SAMPLE_SQL: &str = "
    INSERT INTO wastewater_samples 
    (sample_collection_date, site_name, county, pcr_pathogen_target, pcr_gene_target, normalized_pathogen_concentration, date_updated, poll_timestamp) VALUES 
    (:sample_collection_date, :site_name, :county, :pcr_pathogen_target, :pcr_gene_target, :normalized_pathogen_concentration, :date_updated, :poll_timestamp)";
    let mut insert_stmt = conn.prepare_cached(INSERT_SAMPLE_SQL)?;

    let maybe_existing_sample = select_stmt
        .query_row(
            named_params! {
                ":sample_collection_date": sample.sample_collection_date,
                ":site_name": sample.site_name,
                ":county": sample.county,
                ":pcr_pathogen_target": sample.pcr_pathogen_target,
                ":pcr_gene_target": sample.pcr_gene_target,
            },
            WasteWaterSample::from_row,
        )
        .optional()?;

    match maybe_existing_sample {
        Some(existing_sample) => {
            trace!("Skipping sample insertion because it already exists: New: {sample:?}, Existing: {existing_sample:?}");
            Ok(false)
        }
        None => {
            insert_stmt.execute(named_params! {
                ":sample_collection_date": sample.sample_collection_date,
                ":site_name": sample.site_name,
                ":county": sample.county,
                ":pcr_pathogen_target": sample.pcr_pathogen_target,
                ":pcr_gene_target": sample.pcr_gene_target,
                ":normalized_pathogen_concentration": sample.normalized_pathogen_concentration,
                ":date_updated": sample.date_updated,
                ":poll_timestamp": sample.poll_timestamp,
            })?;

            trace!("Inserted sample: {:?}", sample);
            Ok(true)
        }
    }
}

#[instrument(skip(conn, samples))]
pub fn insert_wastewater_samples<I, S, E>(conn: &mut Connection, samples: I) -> eyre::Result<()>
where
    E: Error,
    S: TryInto<WasteWaterSample, Error = E>,
    I: IntoIterator<Item = S>,
{
    let tx = conn.transaction()?;

    let mut total_sample: usize = 0;
    let mut errors: usize = 0;
    let mut skip: usize = 0;

    for unprocessed_sample in samples {
        total_sample += 1;

        match unprocessed_sample.try_into() {
            Ok(sample) => {
                let inserted = insert_wastewater_sample(&tx, sample)?;
                if !inserted {
                    skip += 1;
                }
            }
            Err(e) => {
                errors += 1;
                error!("Skipping sample due to conversion error: {e}");
            }
        }
    }

    tx.commit()?;

    let total_insertions = total_sample - errors - skip;
    info!("Inserted {total_insertions} records ({errors} errors, {skip} skipped, {total_sample} total)");

    Ok(())
}
