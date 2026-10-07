# Tableau Dashboard

## Included workbook

- **File:** [MSBA405_Final_Project_Dashboard.twb](MSBA405_Final_Project_Dashboard.twb)
- **Original uploaded filename:** `MSBA405 Final Project Dashboard.twb`
- **Format:** Tableau workbook definition (`.twb`), not a packaged workbook (`.twbx`).
- **Saved with:** Tableau 2026.1.0 on Windows, according to workbook metadata.
- **Contents:** One dashboard (`Dashboard 1`) and nine worksheets; five of the
  worksheets appear on the dashboard.
- **Published dashboard URL:** Not supplied.

This public copy retains the dashboard layouts, worksheets, and calculations.
Original Snowflake server, username, role, warehouse, database/schema identifiers,
and local Windows paths have been replaced with placeholders or relative paths.
Configure your own connections before opening or refreshing the dashboard.
The original uploaded attachment is unchanged.

## Worksheets

The workbook includes these named views:

- Avg. Median Income vs. Avg. Log Trips Per Capita
- Car_Share vs. Avg. Trips Per Capita
- No Car Share vs. Trips Per Household
- Trips Per Capita Heatmap
- Walk Share vs. Avg. Trips Per Capita
- no car map
- Sheet 3
- Sheet 4
- Sheet 5

These are worksheet definitions inspected from the workbook XML. This repository
does not include an exported dashboard image or verified rendered results.

## Data connections and required files

The workbook references:

| Source | Workbook reference | Availability |
| --- | --- | --- |
| Snowflake | `TAXI_DEMAND.PUBLIC.FINAL_DATASET` (placeholder) | Requires access to the original source or reconnection to a compatible rebuilt table. |
| Local extract | `data/chicago_taxi_dashboard.hyper` (placeholder) | Not included in this repository or the supplied attachment. |
| Census-tract spatial data | `data/census_tracts/census_tracts.shp` (placeholder) | Not included; obtain the `.shp`, `.shx`, `.dbf`, and `.prj` (if present). |

The source workbook used local Windows paths; this public copy uses relative
placeholder paths. A `.twb`
contains visualization definitions and connection metadata, not the referenced
extract or shapefile. Copying this workbook alone will not make the dashboard
self-contained on another computer.

## Opening and sharing

1. Download the workbook and open it in a compatible Tableau version.
2. Configure the Snowflake connection with your own authorized access and
   reconnect the spatial source. Recreate the extract in Tableau after restoring
   the sources: original extract relation names have also been generalized, so
   simply renaming the original `.hyper` file may not be sufficient.
3. If using the rebuilt pipeline table, verify field mappings, types, and the
   spatial relationship against the workbook before refreshing. The saved
   relationship uses `STR([TRACT_ID]) = [geoid10]`, so preserve compatible
   11-digit tract identifiers. The cloud
   pipeline was not executed as part of adding this workbook.
4. To share a portable dashboard, save a Tableau Packaged Workbook (`.twbx`)
   after restoring the sources and ensure it contains the necessary extract and
   spatial data. Alternatively, add a published dashboard link or a PDF/PNG
   export for a viewable work sample.

The original pipeline and this supplied workbook are retained as project
artifacts. Original pipeline attribution and commit history are preserved; no
claim of sole authorship is made.
