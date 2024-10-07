BEGIN;

CREATE TABLE IF NOT EXISTS wastewater_samples (
    sample_collection_date TEXT NOT NULL,
    site_name TEXT NOT NULL,
    county TEXT NOT NULL,
    pcr_pathogen_target TEXT NOT NULL,
    pcr_gene_target TEXT NOT NULL,
    normalized_pathogen_concentration REAL NOT NULL,
    date_updated TEXT NOT NULL,
    poll_timestamp INTEGER NOT NULL,
    PRIMARY KEY (sample_collection_date, site_name, county, pcr_pathogen_target, pcr_gene_target)
);

-- Create an index on the poll_timestamp for efficient querying of recent data
CREATE INDEX IF NOT EXISTS idx_wastewater_samples_poll_timestamp ON wastewater_samples(poll_timestamp);

-- Create an index on the date_updated for efficient querying of recently updated data
CREATE INDEX IF NOT EXISTS idx_wastewater_samples_date_updated ON wastewater_samples(date_updated);

COMMIT;