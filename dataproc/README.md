# Dataproc PySpark Job

`clean_and_aggregate_v3.py` is the production Spark entry point. It accepts a
bucket with `--bucket` or explicit `--taxi-input`, `--acs-input`, and
`--output-base` paths. Supplying `--bucket gs://YOUR_BUCKET` selects the standard
project layout automatically.

The job requires the recovered Chicago Taxi fields used for timestamps, pickup
and drop-off tracts, duration, distance, fare, tips, and total. It retains 2017
trips with both tract identifiers, positive duration/distance/total, nonnegative
fare/tips, and a total at least as large as fare. It then removes the recovered
outlier conditions: durations of four hours or more, distances of 100 miles or
more, totals of $500 or more, and speeds outside the open interval 1–80 mph.
Pickup tract is normalized to an 11-character GEOID, month is extracted as 1–12,
and tip rate is calculated only for positive fares.

ACS `geo_id` values are standardized by extracting their final 11 digits. Only
Cook County GEOIDs beginning `17031` are retained. Positive population,
household, commuter, and worker denominators are required. The job derives the
recovered shares for car, public transit, walking, working from home, no-car
households, children, school enrollment, and bachelor's degree attainment; rows
with shares outside 0–1 are rejected.

Taxi trips are grouped by pickup tract and month. The job calculates trip count,
average miles, average duration in minutes, average total, average tip rate, and
total revenue. An inner join adds ACS characteristics, followed by trips per
capita and trips per household. The final output has a stable 23-column order
defined by `FINAL_COLUMNS`.

Standard outputs are overwritten on each run:

- `gs://YOUR_BUCKET/processed/taxi_cleaned/` — cleaned trip-level Parquet
- `gs://YOUR_BUCKET/processed/census_cleaned/` — cleaned tract-level Parquet
- `gs://YOUR_BUCKET/processed/taxi_census_tract_month/` — final Parquet
- `gs://YOUR_BUCKET/processed/final_dataset_single_csv/` — one-part, headered CSV

The one-part CSV is intentionally optimized for the Snowflake load, not for
large-scale downstream Spark reads. Missing required columns raise a clear error
before transformations proceed.
