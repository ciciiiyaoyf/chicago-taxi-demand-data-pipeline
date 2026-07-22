# Pipeline Setup and Operation

## Required tools

Run from Google Cloud Shell, Linux, macOS, WSL, or another Bash environment with:

- Bash 4 or newer
- Google Cloud CLI (`gcloud`) with the Dataproc component/API available
- Snowflake CLI (`snow`)
- Python 3 for the offline smoke test

The script validates commands instead of installing software during a run.

## Google Cloud prerequisites

1. Authenticate and select an account:

   ```bash
   gcloud auth login
   gcloud auth list
   ```

2. Ensure the project has the Dataproc API enabled.
3. Create a GCS bucket containing the raw Parquet paths from
   [data_setup.md](data_setup.md).
4. Create and start a Dataproc cluster in the configured region. Its service
   account needs read/write access to the bucket.
5. Ensure the local user can describe the project/cluster, upload to the bucket,
   and submit Dataproc jobs.

## Configure and execute

Export every variable shown in the root [README](../README.md#environment-variables)
in the current shell. `PYSPARK_FILE` is optional and defaults to
`dataproc/clean_and_aggregate_v3.py`; all other listed variables are required.
The script does not source an environment template.

Run from the repository root:

```bash
python tests/smoke_test.py
chmod +x pipeline.sh
./pipeline.sh
```

The script uploads the local PySpark file to `gs://YOUR_BUCKET/code/`, submits it
with the configured bucket argument, and waits for completion. The job overwrites
the four documented processed paths. The script then requires exactly one Spark
`part-*` file in `processed/final_dataset_single_csv/` and loads that exact file
through Snowflake.

## Expected output

Successful output ends with the final Parquet path, exact CSV URI, and qualified
Snowflake table. Snowflake query output also includes `ROWS_LOADED` and a sample
of at most ten rows.

## Troubleshooting

- **No active account:** run `gcloud auth login` in the same environment.
- **Project/bucket/cluster not found:** check spelling, region, active account,
  IAM permissions, and whether the cluster is running.
- **Missing Spark columns:** compare the exported BigQuery schemas with the
  required-column lists at the top of the production PySpark file.
- **No or multiple CSV part files:** confirm the Spark job completed and that the
  output uses `coalesce(1)`. Clear stale objects only after verifying the bucket.
- **Snowflake connection failure:** check account locator, username, password,
  role, warehouse, MFA/authentication policy, and network access.
- **Snowflake file not found:** confirm the external stage root maps to the GCS
  bucket's `processed/` directory and permits Snowflake to list/read objects.
- **Insufficient Snowflake privilege:** grant the configured role the privileges
  summarized in [snowflake_setup.md](snowflake_setup.md).

Temporary generated SQL is removed on both success and failure. The script never
prints the password.
