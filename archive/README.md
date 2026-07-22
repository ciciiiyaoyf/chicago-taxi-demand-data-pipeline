# Recovered File Inventory

This directory records how the supplied working files were handled. Original
files containing account-specific infrastructure or credentials were not copied
verbatim into the repository. Their reusable logic was recovered into the
production files, and the originals remain in the owner's external recovery
folder.

| Recovered file | Disposition |
| --- | --- |
| `clean_and_aggregate_v3.py.txt` | Its cleaning, ACS ratios, six taxi aggregates, join, normalized metrics, output paths, and 23-column order form the production PySpark job. Hard-coded paths were removed. |
| `pipeline.sh.txt` | Its upload, Dataproc submission, Snowflake DDL/load, and validation flow were rebuilt in `pipeline.sh`. Embedded credentials and personal object names were deliberately not retained. |
| `tract_segmentation_features_monthly.py` | Its argument parsing, required-column validation, and robust 11-digit ACS GEOID extraction informed the production job. Its incompatible experimental segmentation schema and monthly loop were not retained. |
| `acs_ca_2017.py` | Inspected; it is byte-for-byte identical to the recovered cloud smoke test. Removed as an exact duplicate. |
| `smoke_test.py` | Its basic path/file accessibility intent was replaced by offline repository smoke tests. The old script required live GCP and was account-specific. |
| `taxi_monthly_ca_2017.py` | Inspected; it is byte-for-byte identical to `acs_ca_2017.py` and the old `smoke_test.py`. Removed as an exact duplicate. |
| `processed_final_dataset_single_csv - Sheet 1 - processed_final_datas.csv` | Used locally to confirm the 23 output columns, their order, and representative types. It had 4,061 data rows plus a Google Sheets wrapper row and is excluded as generated data. |
| `testpipe.txt` and `testpy.txt` | Both were zero-byte temporary files and were removed. |
| root `dele` | A pre-existing four-byte placeholder from the empty repository; moved here as a non-production artifact with a normalized line ending. |

The old shell script exposed a real-looking password and account values. Those
values must be considered compromised and rotated; they do not appear in this
repository.
