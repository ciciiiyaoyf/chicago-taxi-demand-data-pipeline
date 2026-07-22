# Snowflake Load

**The SQL file does not need to be executed separately in the standard workflow because equivalent SQL logic is executed automatically by `pipeline.sh`.**

`load_final_dataset.sql` is a credential-free reference containing placeholders.
It selects the configured role and warehouse, creates the database and schema if
the role permits, creates the CSV file format, replaces the destination table,
loads the exact Spark `part-*.csv` file, and runs row-count and ten-row sample
queries.

The table has the same 23 columns, order, and compatible types as
`dataproc/clean_and_aggregate_v3.py`. `TRACT_ID` is `VARCHAR(11)` because a GEOID
is an identifier whose leading zeros must be preserved. Count fields are fixed
point numbers; calculated measures and ACS rates use floating-point columns.

The pipeline assumes `SNOWFLAKE_STAGE` is an existing external stage whose root
maps to `gs://YOUR_BUCKET/processed/`. The Snowflake storage integration, cloud
permissions, and stage are one-time infrastructure and are not created by the
script. See [the setup guide](../docs/snowflake_setup.md).
