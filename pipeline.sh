#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYSPARK_FILE="${PYSPARK_FILE:-dataproc/clean_and_aggregate_v3.py}"
SQL_FILE=""

log() {
    printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

fail() {
    printf '\nERROR: %s\n' "$*" >&2
    exit 1
}

cleanup() {
    local status=$?
    if [[ -n "${SQL_FILE}" && -f "${SQL_FILE}" ]]; then
        rm -f -- "${SQL_FILE}"
    fi
    if (( status != 0 )); then
        printf '\nPipeline failed with exit code %d. Review the last command output.\n' "${status}" >&2
    fi
}
trap cleanup EXIT

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

require_env() {
    local name=$1
    [[ -n "${!name:-}" ]] || fail "Required environment variable is not set: ${name}"
}

require_identifier() {
    local name=$1
    local value=${!name}
    [[ "${value}" =~ ^[A-Za-z_][A-Za-z0-9_\$]*$ ]] \
        || fail "${name} must be an unquoted Snowflake identifier: ${value}"
}

require_qualified_identifier() {
    local name=$1
    local value=${!name}
    [[ "${value}" =~ ^[A-Za-z_][A-Za-z0-9_\$]*(\.[A-Za-z_][A-Za-z0-9_\$]*){0,2}$ ]] \
        || fail "${name} must be a one- to three-part Snowflake identifier: ${value}"
}

validate_configuration() {
    local required_vars=(
        GCP_PROJECT_ID GCS_BUCKET DATAPROC_CLUSTER DATAPROC_REGION
        SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD SNOWFLAKE_ROLE
        SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA
        SNOWFLAKE_TABLE SNOWFLAKE_FILE_FORMAT SNOWFLAKE_STAGE
    )
    local var
    for var in "${required_vars[@]}"; do
        require_env "${var}"
    done

    for var in SNOWFLAKE_ROLE SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE \
        SNOWFLAKE_SCHEMA SNOWFLAKE_TABLE SNOWFLAKE_FILE_FORMAT; do
        require_identifier "${var}"
    done
    require_qualified_identifier SNOWFLAKE_STAGE

    require_command gcloud
    require_command snow
    require_command mktemp
    require_command env

    if [[ "${PYSPARK_FILE}" = /* ]]; then
        PYSPARK_PATH="${PYSPARK_FILE}"
    else
        PYSPARK_PATH="${SCRIPT_DIR}/${PYSPARK_FILE}"
    fi
    [[ -f "${PYSPARK_PATH}" ]] || fail "PySpark file not found: ${PYSPARK_PATH}"

    GCS_BUCKET="${GCS_BUCKET#gs://}"
    GCS_BUCKET="${GCS_BUCKET%/}"
    [[ -n "${GCS_BUCKET}" && "${GCS_BUCKET}" != */* ]] \
        || fail "GCS_BUCKET must be a bucket name or gs:// bucket URI"

    BUCKET_URI="gs://${GCS_BUCKET}"
    CODE_URI="${BUCKET_URI}/code/$(basename -- "${PYSPARK_PATH}")"
    FINAL_CSV_URI="${BUCKET_URI}/processed/final_dataset_single_csv"
}

validate_gcp() {
    log "Validating Google Cloud authentication and resources"
    local active_account
    local cluster_state
    active_account="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' --limit=1)"
    [[ -n "${active_account}" ]] || fail "No active gcloud account. Run: gcloud auth login"
    gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectId)' >/dev/null
    gcloud storage ls "${BUCKET_URI}" >/dev/null
    cluster_state="$(gcloud dataproc clusters describe "${DATAPROC_CLUSTER}" \
        --project="${GCP_PROJECT_ID}" \
        --region="${DATAPROC_REGION}" \
        --format='value(status.state)')"
    [[ "${cluster_state}" == "RUNNING" ]] \
        || fail "Dataproc cluster ${DATAPROC_CLUSTER} is ${cluster_state:-UNKNOWN}, not RUNNING"
}

run_dataproc() {
    log "Uploading PySpark job to ${CODE_URI}"
    gcloud storage cp "${PYSPARK_PATH}" "${CODE_URI}"

    log "Submitting Dataproc job (this command waits for completion)"
    gcloud dataproc jobs submit pyspark "${CODE_URI}" \
        --project="${GCP_PROJECT_ID}" \
        --cluster="${DATAPROC_CLUSTER}" \
        --region="${DATAPROC_REGION}" \
        -- \
        --bucket "${BUCKET_URI}"
}

locate_csv_part() {
    log "Locating the final Spark CSV part file"
    local listing
    listing="$(gcloud storage ls "${FINAL_CSV_URI}/part-*")" \
        || fail "No CSV part file found under ${FINAL_CSV_URI}"
    mapfile -t CSV_PARTS <<<"${listing}"
    (( ${#CSV_PARTS[@]} == 1 )) \
        || fail "Expected one CSV part file, found ${#CSV_PARTS[@]} under ${FINAL_CSV_URI}"
    CSV_PART_URI="${CSV_PARTS[0]}"
    CSV_PART_NAME="${CSV_PART_URI##*/}"
    [[ "${CSV_PART_NAME}" =~ ^part-[A-Za-z0-9._-]+$ ]] \
        || fail "Unexpected Spark part filename: ${CSV_PART_NAME}"
    log "Found ${CSV_PART_URI}"
}

create_load_sql() {
    SQL_FILE="$(mktemp "${TMPDIR:-/tmp}/mgmt405-snowflake.XXXXXX.sql")"
    cat >"${SQL_FILE}" <<SQL
USE ROLE ${SNOWFLAKE_ROLE};
USE WAREHOUSE ${SNOWFLAKE_WAREHOUSE};

CREATE DATABASE IF NOT EXISTS ${SNOWFLAKE_DATABASE};
CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA};
USE DATABASE ${SNOWFLAKE_DATABASE};
USE SCHEMA ${SNOWFLAKE_SCHEMA};

CREATE OR REPLACE FILE FORMAT ${SNOWFLAKE_FILE_FORMAT}
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('', 'NULL', 'null')
  SKIP_BLANK_LINES = TRUE;

CREATE OR REPLACE TABLE ${SNOWFLAKE_TABLE} (
  TRACT_ID VARCHAR(11),
  MONTH NUMBER(2,0),
  TRIP_COUNT NUMBER(38,0),
  TRIPS_PER_CAPITA FLOAT,
  TRIPS_PER_HOUSEHOLD FLOAT,
  TOTAL_REVENUE FLOAT,
  AVG_TRIP_MILES FLOAT,
  AVG_TRIP_MINUTES FLOAT,
  AVG_TRIP_TOTAL FLOAT,
  AVG_TIP_RATE FLOAT,
  TOTAL_POP NUMBER(38,0),
  HOUSEHOLDS NUMBER(38,0),
  COMMUTERS_16_OVER NUMBER(38,0),
  CAR_SHARE FLOAT,
  TRANSIT_SHARE FLOAT,
  WALK_SHARE FLOAT,
  WORK_HOME_SHARE FLOAT,
  NO_CAR_SHARE FLOAT,
  CHILDREN_SHARE FLOAT,
  IN_SCHOOL_SHARE FLOAT,
  MEDIAN_INCOME FLOAT,
  INCOME_PER_CAPITA FLOAT,
  BACHELORS_RATE FLOAT
);

COPY INTO ${SNOWFLAKE_TABLE}
FROM @${SNOWFLAKE_STAGE}/final_dataset_single_csv/${CSV_PART_NAME}
FILE_FORMAT = (FORMAT_NAME = ${SNOWFLAKE_FILE_FORMAT})
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;

SELECT COUNT(*) AS ROWS_LOADED FROM ${SNOWFLAKE_TABLE};
SELECT * FROM ${SNOWFLAKE_TABLE} LIMIT 10;
SQL
}

run_snowflake() {
    log "Testing the Snowflake temporary connection"
    # Snowflake CLI reads the exported SNOWFLAKE_* variables. In particular,
    # the password stays in the environment and is not put on the command line.
    # Database/schema are omitted during bootstrap so the SQL can create them.
    env -u SNOWFLAKE_DATABASE -u SNOWFLAKE_SCHEMA \
        snow connection test --temporary-connection

    create_load_sql
    log "Loading ${CSV_PART_NAME} and running validation queries"
    env -u SNOWFLAKE_DATABASE -u SNOWFLAKE_SCHEMA \
        snow sql --temporary-connection --filename "${SQL_FILE}"
}

main() {
    log "Starting Chicago Taxi Demand Pipeline"
    validate_configuration
    validate_gcp
    run_dataproc
    locate_csv_part
    run_snowflake

    log "Pipeline completed successfully"
    printf 'GCS Parquet: %s\n' "${BUCKET_URI}/processed/taxi_census_tract_month/"
    printf 'GCS CSV:     %s\n' "${CSV_PART_URI}"
    printf 'Snowflake:   %s.%s.%s\n' \
        "${SNOWFLAKE_DATABASE}" "${SNOWFLAKE_SCHEMA}" "${SNOWFLAKE_TABLE}"
}

main "$@"
