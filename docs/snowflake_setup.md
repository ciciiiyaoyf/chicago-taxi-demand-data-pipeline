# Snowflake Setup

The automated load needs an existing Snowflake account, role, warehouse, and
external stage. The reference SQL normally does not need to be run separately;
`pipeline.sh` validates configuration and executes equivalent generated SQL.

## Required objects and privileges

Choose a role with permission to:

- use the configured warehouse and resume it if required;
- create or use the configured database and schema;
- create a file format and table in that schema;
- use the external stage and its storage integration;
- select from and load into the destination table.

For a least-privilege deployment, an administrator should create the database,
schema, storage integration, and stage once, then grant only the necessary usage
and object privileges to the pipeline role. The recovered script used a broad
administrative role; the rebuilt project does not require or recommend that
choice.

## External stage prerequisite

Create a cloud storage integration for GCS according to your Snowflake account's
security process, grant its generated service identity access to the bucket, and
create an external stage. The stage URL must be rooted here:

```text
gs://YOUR_BUCKET_NAME/processed/
```

Conceptually, the stage should resemble the following, but integration names and
privilege grants are organization-specific:

```sql
CREATE STAGE YOUR_DATABASE.YOUR_SCHEMA.YOUR_STAGE
  URL = 'gcs://YOUR_BUCKET_NAME/processed/'
  STORAGE_INTEGRATION = YOUR_GCS_STORAGE_INTEGRATION;
```

Do not put cloud credentials in this repository. `pipeline.sh` does not create
the storage integration or stage. Test that Snowflake can see the output folder:

```sql
LIST @YOUR_DATABASE.YOUR_SCHEMA.YOUR_STAGE/final_dataset_single_csv/;
```

Set `SNOWFLAKE_STAGE` to the one-, two-, or three-part stage identifier. A fully
qualified value such as `YOUR_DATABASE.YOUR_SCHEMA.YOUR_STAGE` is safest.

## Pipeline environment

Export the Snowflake variables listed in the root
[README](../README.md#environment-variables). The standard password flow uses a
Snowflake CLI temporary connection. `SNOWFLAKE_PASSWORD` remains an environment
variable and is neither passed as a command-line argument nor stored in a named
CLI connection. Authentication policies that require another method must be
adapted and tested by the account administrator.

The pipeline creates or replaces the file format and destination table, loads
the exact Spark part file with `COPY INTO`, then runs:

```sql
SELECT COUNT(*) AS ROWS_LOADED FROM YOUR_TABLE;
SELECT * FROM YOUR_TABLE LIMIT 10;
```

Use [snowflake/load_final_dataset.sql](../snowflake/load_final_dataset.sql) only
as a review aid or for manual troubleshooting after replacing its placeholders.
