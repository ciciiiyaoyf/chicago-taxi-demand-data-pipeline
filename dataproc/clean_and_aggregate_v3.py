"""Build the 2017 Chicago taxi-demand tract-month analytical dataset.

This production job recovers the compatible cleaning, aggregation, and feature
logic from the original MGMT 405 project. Account-specific paths are supplied
at runtime; no project, bucket, or credential is embedded in this file.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


TAXI_REQUIRED_COLUMNS = (
    "trip_start_timestamp",
    "pickup_census_tract",
    "dropoff_census_tract",
    "trip_seconds",
    "trip_miles",
    "fare",
    "tips",
    "trip_total",
)

ACS_REQUIRED_COLUMNS = (
    "geo_id",
    "commuters_16_over",
    "workers_16_and_over",
    "households",
    "total_pop",
    "commuters_by_car_truck_van",
    "commuters_by_public_transportation",
    "walked_to_work",
    "worked_at_home",
    "no_cars",
    "bachelors_degree_2",
    "children",
    "in_school",
    "median_income",
    "income_per_capita",
)

# This order is the CSV contract consumed by Snowflake. Keep it synchronized
# with snowflake/load_final_dataset.sql and pipeline.sh.
FINAL_COLUMNS = (
    "tract_id",
    "month",
    "trip_count",
    "trips_per_capita",
    "trips_per_household",
    "total_revenue",
    "avg_trip_miles",
    "avg_trip_minutes",
    "avg_trip_total",
    "avg_tip_rate",
    "total_pop",
    "households",
    "commuters_16_over",
    "car_share",
    "transit_share",
    "walk_share",
    "work_home_share",
    "no_car_share",
    "children_share",
    "in_school_share",
    "median_income",
    "income_per_capita",
    "bachelors_rate",
)


@dataclass(frozen=True)
class PipelinePaths:
    taxi_input: str
    acs_input: str
    taxi_cleaned: str
    census_cleaned: str
    final_parquet: str
    final_csv: str


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and join 2017 Chicago Taxi and ACS tract data."
    )
    parser.add_argument(
        "--bucket",
        help="GCS bucket name or gs:// URI used to derive default paths.",
    )
    parser.add_argument("--taxi-input", help="Full Parquet path for taxi input.")
    parser.add_argument("--acs-input", help="Full Parquet path for ACS input.")
    parser.add_argument(
        "--output-base",
        help="Output root; defaults to gs://BUCKET/processed when --bucket is set.",
    )
    return parser.parse_args(argv)


def normalize_bucket(value: str) -> str:
    bucket = value.strip().rstrip("/")
    if not bucket:
        raise ValueError("--bucket cannot be empty")
    if not bucket.startswith("gs://"):
        bucket = f"gs://{bucket}"
    return bucket


def resolve_paths(args: argparse.Namespace) -> PipelinePaths:
    bucket = normalize_bucket(args.bucket) if args.bucket else None
    taxi_input = args.taxi_input or (
        f"{bucket}/chicago_taxi/chicago_taxi_2017/" if bucket else None
    )
    acs_input = args.acs_input or (
        f"{bucket}/ACS/acs_censustract_2017_5yr/" if bucket else None
    )
    output_base = args.output_base or (f"{bucket}/processed" if bucket else None)

    missing = [
        name
        for name, value in (
            ("--taxi-input", taxi_input),
            ("--acs-input", acs_input),
            ("--output-base", output_base),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            "Provide --bucket or explicitly supply all missing paths: "
            + ", ".join(missing)
        )

    output_base = output_base.rstrip("/")
    return PipelinePaths(
        taxi_input=taxi_input,
        acs_input=acs_input,
        taxi_cleaned=f"{output_base}/taxi_cleaned/",
        census_cleaned=f"{output_base}/census_cleaned/",
        final_parquet=f"{output_base}/taxi_census_tract_month/",
        final_csv=f"{output_base}/final_dataset_single_csv/",
    )


def require_columns(frame: DataFrame, required: Sequence[str], dataset: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{dataset} data is missing required columns: {missing}")


def safe_ratio(numerator: str, denominator: str):
    """Return a double ratio only when the denominator is strictly positive."""
    return F.when(
        F.col(denominator) > 0,
        F.col(numerator).cast("double") / F.col(denominator).cast("double"),
    ).otherwise(F.lit(None).cast("double"))


def clean_taxi(taxi: DataFrame) -> DataFrame:
    require_columns(taxi, TAXI_REQUIRED_COLUMNS, "Taxi")

    cleaned = (
        taxi.filter(
            F.col("pickup_census_tract").isNotNull()
            & F.col("dropoff_census_tract").isNotNull()
            & (F.year("trip_start_timestamp") == 2017)
            & (F.col("trip_seconds") > 0)
            & (F.col("trip_miles") > 0)
            & (F.col("fare") >= 0)
            & (F.col("tips") >= 0)
            & (F.col("trip_total") > 0)
            & (F.col("trip_total") >= F.col("fare"))
        )
        .withColumn(
            "speed_mph",
            F.col("trip_miles") / (F.col("trip_seconds") / F.lit(3600.0)),
        )
        .filter(
            (F.col("trip_seconds") < 4 * 3600)
            & (F.col("trip_miles") < 100)
            & (F.col("trip_total") < 500)
            & (F.col("speed_mph") > 1.0)
            & (F.col("speed_mph") < 80.0)
        )
        .withColumn("tract_id", F.format_string("%011d", F.col("pickup_census_tract").cast("long")))
        .filter(F.col("tract_id").rlike(r"^\d{11}$"))
        .withColumn("month", F.month("trip_start_timestamp").cast("integer"))
        # The recovered script divided by fare unconditionally even though it
        # allowed zero fares. Null-safe division preserves those trips while
        # excluding undefined rates from Spark's average.
        .withColumn("tip_rate", safe_ratio("tips", "fare"))
        .select(
            "tract_id",
            "trip_start_timestamp",
            "month",
            F.col("trip_seconds").cast("double").alias("trip_seconds"),
            F.col("trip_miles").cast("double").alias("trip_miles"),
            F.col("fare").cast("double").alias("fare"),
            F.col("tips").cast("double").alias("tips"),
            F.col("trip_total").cast("double").alias("trip_total"),
            "tip_rate",
        )
    )
    return cleaned


def clean_census(census: DataFrame) -> DataFrame:
    require_columns(census, ACS_REQUIRED_COLUMNS, "ACS")

    # geo_id may be an 11-digit GEOID or a prefixed Census identifier such as
    # 14000US17031010100. Extracting the final 11 digits handles both forms.
    cleaned = (
        census.withColumn(
            "tract_id",
            F.regexp_extract(F.col("geo_id").cast("string"), r"(\d{11})$", 1),
        )
        .filter(F.col("tract_id").startswith("17031"))
        .filter(
            F.col("commuters_16_over").isNotNull()
            & F.col("workers_16_and_over").isNotNull()
            & F.col("households").isNotNull()
            & F.col("total_pop").isNotNull()
            & (F.col("commuters_16_over") > 0)
            & (F.col("workers_16_and_over") > 0)
            & (F.col("households") > 0)
            & (F.col("total_pop") > 0)
        )
        .withColumn("car_share", safe_ratio("commuters_by_car_truck_van", "commuters_16_over"))
        .withColumn(
            "transit_share",
            safe_ratio("commuters_by_public_transportation", "commuters_16_over"),
        )
        .withColumn("walk_share", safe_ratio("walked_to_work", "commuters_16_over"))
        .withColumn("work_home_share", safe_ratio("worked_at_home", "workers_16_and_over"))
        .withColumn("no_car_share", safe_ratio("no_cars", "households"))
        .withColumn("bachelors_rate", safe_ratio("bachelors_degree_2", "total_pop"))
        .withColumn("children_share", safe_ratio("children", "total_pop"))
        .withColumn("in_school_share", safe_ratio("in_school", "total_pop"))
        .filter(
            F.col("car_share").between(0.0, 1.0)
            & F.col("transit_share").between(0.0, 1.0)
            & F.col("walk_share").between(0.0, 1.0)
            & F.col("work_home_share").between(0.0, 1.0)
            & F.col("no_car_share").between(0.0, 1.0)
            & F.col("bachelors_rate").between(0.0, 1.0)
            & F.col("children_share").between(0.0, 1.0)
            & F.col("in_school_share").between(0.0, 1.0)
        )
        .select(
            "tract_id",
            F.col("total_pop").cast("long").alias("total_pop"),
            F.col("households").cast("long").alias("households"),
            F.col("commuters_16_over").cast("long").alias("commuters_16_over"),
            "car_share",
            "transit_share",
            "walk_share",
            "work_home_share",
            "no_car_share",
            "children_share",
            "in_school_share",
            F.col("median_income").cast("double").alias("median_income"),
            F.col("income_per_capita").cast("double").alias("income_per_capita"),
            "bachelors_rate",
        )
    )
    return cleaned


def build_final_dataset(taxi_clean: DataFrame, census_clean: DataFrame) -> DataFrame:
    taxi_agg = (
        taxi_clean.groupBy("tract_id", "month")
        .agg(
            F.count(F.lit(1)).cast("long").alias("trip_count"),
            F.avg("trip_miles").alias("avg_trip_miles"),
            F.avg("trip_seconds").alias("avg_trip_seconds"),
            F.avg("trip_total").alias("avg_trip_total"),
            F.avg("tip_rate").alias("avg_tip_rate"),
            F.sum("trip_total").alias("total_revenue"),
        )
        .withColumn("avg_trip_minutes", F.col("avg_trip_seconds") / F.lit(60.0))
        .drop("avg_trip_seconds")
    )

    final_df = (
        taxi_agg.join(census_clean, on="tract_id", how="inner")
        .withColumn("trips_per_capita", safe_ratio("trip_count", "total_pop"))
        .withColumn("trips_per_household", safe_ratio("trip_count", "households"))
        .select(*FINAL_COLUMNS)
    )
    return final_df


def run(spark: SparkSession, paths: PipelinePaths) -> None:
    print(f"Reading taxi Parquet from {paths.taxi_input}")
    print(f"Reading ACS Parquet from {paths.acs_input}")
    taxi = spark.read.parquet(paths.taxi_input)
    census = spark.read.parquet(paths.acs_input)

    taxi_clean = clean_taxi(taxi)
    census_clean = clean_census(census)
    final_df = build_final_dataset(taxi_clean, census_clean)

    print(f"Writing cleaned taxi Parquet to {paths.taxi_cleaned}")
    taxi_clean.write.mode("overwrite").parquet(paths.taxi_cleaned)

    print(f"Writing cleaned ACS Parquet to {paths.census_cleaned}")
    census_clean.write.mode("overwrite").parquet(paths.census_cleaned)

    print(f"Writing final Parquet to {paths.final_parquet}")
    final_df.write.mode("overwrite").parquet(paths.final_parquet)

    print(f"Writing one-part final CSV to {paths.final_csv}")
    (
        final_df.orderBy("tract_id", "month")
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("nullValue", "")
        .csv(paths.final_csv)
    )

    print("Pipeline output columns: " + ", ".join(FINAL_COLUMNS))


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    paths = resolve_paths(args)
    spark = SparkSession.builder.appName("mgmt405_chicago_taxi_demand_2017").getOrCreate()
    try:
        run(spark, paths)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
