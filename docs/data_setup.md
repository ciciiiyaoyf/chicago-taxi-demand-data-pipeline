# Data Setup

This one-time process places the two source datasets in the exact paths expected
by the default Spark configuration. Run the SQL in BigQuery after replacing all
`YOUR_*` values. The destination dataset and GCS bucket should use compatible
locations.

## 1. Create a BigQuery dataset

```sql
CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.mgmt405_raw`
OPTIONS(location = 'YOUR_BIGQUERY_LOCATION');
```

## 2. Copy the ACS 2017 tract table

The recovered processing code expects the full ACS tract schema because it uses
specific commuting, population, household, education, and income columns. Copy
the table without narrowing its fields:

```sql
CREATE OR REPLACE TABLE
  `YOUR_GCP_PROJECT_ID.mgmt405_raw.acs_censustract_2017_5yr` AS
SELECT *
FROM `bigquery-public-data.census_bureau_acs.censustract_2017_5yr`;
```

If that public table is not visible, locate the Census Bureau ACS public dataset
in BigQuery Explorer and confirm the current 2017 five-year tract table name.
Do not substitute a county- or block-group-level table.

## 3. Create the 2017 Chicago taxi table

The production job needs the source fields named in `TAXI_REQUIRED_COLUMNS`; no
unsupported source-field selection has been invented. Preserve all columns and
filter only the trip start timestamp:

```sql
CREATE OR REPLACE TABLE
  `YOUR_GCP_PROJECT_ID.mgmt405_raw.chicago_taxi_2017` AS
SELECT *
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE trip_start_timestamp >= TIMESTAMP('2017-01-01')
  AND trip_start_timestamp < TIMESTAMP('2018-01-01');
```

Confirm in BigQuery Explorer that the public source and required field names are
still available before running the query.

## 4. Export both tables as Parquet

Create `YOUR_BUCKET_NAME` first and grant the BigQuery export identity permission
to write objects. BigQuery export URIs require a wildcard.

```sql
EXPORT DATA OPTIONS (
  uri = 'gs://YOUR_BUCKET_NAME/ACS/acs_censustract_2017_5yr/*.parquet',
  format = 'PARQUET',
  compression = 'SNAPPY',
  overwrite = true
) AS
SELECT *
FROM `YOUR_GCP_PROJECT_ID.mgmt405_raw.acs_censustract_2017_5yr`;
```

```sql
EXPORT DATA OPTIONS (
  uri = 'gs://YOUR_BUCKET_NAME/chicago_taxi/chicago_taxi_2017/*.parquet',
  format = 'PARQUET',
  compression = 'SNAPPY',
  overwrite = true
) AS
SELECT *
FROM `YOUR_GCP_PROJECT_ID.mgmt405_raw.chicago_taxi_2017`;
```

The resulting folder names must be exactly:

```text
gs://YOUR_BUCKET_NAME/ACS/acs_censustract_2017_5yr/
gs://YOUR_BUCKET_NAME/chicago_taxi/chicago_taxi_2017/
```

Verify both exports before running Dataproc:

```bash
gcloud storage ls 'gs://YOUR_BUCKET_NAME/ACS/acs_censustract_2017_5yr/**'
gcloud storage ls 'gs://YOUR_BUCKET_NAME/chicago_taxi/chicago_taxi_2017/**'
```
