# Harsh-data-engineering-assignment-submission

#  Data Engineer Assignment — Satellite Intelligence Pipeline

> Real field data is never clean. The job isn't to find a clean dataset —
> it's to make defensible decisions fast about a messy one.


All audit findings, cleaning decisions, quarantine logic, and analysis interpretation are my own. Every line of code was reviewed, understood, and run by me on Databricks Community Edition.


## What I Found in 5 Minutes of Looking at the Raw Data

Before writing a single line of code, I opened both CSVs and looked at them.

parcel_readings.csv immediately showed three problems visible to the naked eye:

- The 'date' column had three different formats in the same column — '2026-01-01', '16/05/2026, and '15-Jan-2026'. Any naive date parser would silently corrupt at least one of them.
- The `sensor_status` column had what looked like three values — OK, ERROR, and blank — but was actually ten: `OK`, `ok`, `OK ` (trailing space), `OK` (different byte encoding), `Error`, `ERROR`, `error`, `NA`, `NaN`, `NULL`, and genuine blanks. Excel confirmed 3,064 OK records across all variants. PySpark matched the same after normalisation.
- Two parcel IDs — `PARCEL_098` and `PARCEL_099` — appeared in readings but had no entry in metadata. These are orphan records: sensors sending data for parcels that don't officially exist in the system yet.

This 5-minute look shaped every decision in the pipeline.

---

## Architecture


Raw CSVs (parcel_readings + parcel_metadata)
            │
            ▼
    ┌───────────────┐
    │  BRONZE LAYER │  Ingest raw data, enforce schema
    │  3,447 rows   │
    └──────┬────────┘
           │
           ▼
    ┌──────────────────────────────────────┐
    │          SILVER LAYER                │
    │  Clean + Validate + Quarantine       │
    │                                      │
    │  Clean path  →  3,399 rows           │
    │  Quarantine  →     48 rows           │
    │    ├── 8  exact duplicates           │
    │    └── 40 orphan records             │
    └──────┬───────────────────────────────┘
           │
           ▼
    ┌───────────────┐
    │  GOLD LAYER   │  NDVI analysis before/after sowing
    │  per crop     │  sensor_status = OK rows only
    └───────────────┘
            │
            ▼
    Delta Tables (Databricks)
    ├── cleaned_parcel_timeseries
    ├── quarantine_records
    └── ndvi_analysis


**Why Bronze → Silver → Gold?**
Raw data is never overwritten. If the cleaning logic has a bug, reprocessing from Bronze requires no re-ingestion from source. The quarantine table means no row is ever silently dropped — every rejected record is stored with a reason and timestamp.



## Data Quality Audit

### parcel_readings.csv — 3,447 rows × 6 columns

| # | Issue | Count | % | Decision | Justification |
|---|-------|-------|---|----------|---------------|
| 1 | Mixed date formats — three formats: `yyyy-MM-dd`, `dd/MM/yyyy`, `dd-MMM-yyyy` | ~520 rows | ~15% | **Repair** | All rows carry a valid date, just encoded inconsistently. Used `try_to_date` with `coalesce` across three format patterns. Dropping would silently shrink the dataset. |
| 2 | `sensor_status` case and whitespace variants — `ok`, `OK `, `Error`, `ERROR`, `error` | 310 rows | 9% | **Repair** | Pure cosmetic inconsistency. `upper(trim())` normalises all variants to `OK` or `ERROR` with no information loss. |
| 3 | `sensor_status` string nulls — `NA`, `NaN`, `NULL`, and genuine blanks | 137 rows | 4% | **Flag as UNKNOWN** | Cannot determine if sensor was healthy or not. Neither OK nor ERROR is a safe assumption. A third category `UNKNOWN` means analysis filters on `sensor_status = OK` safely exclude these rows without guesswork. |
| 4 | NDVI out of physical range `[-1, 1]` | 104 rows | 3% | **Null + flag** | Values outside `[-1, 1]` are physically impossible — instrument error or transmission noise. Set `ndvi_value` to NULL and added boolean `ndvi_flag` column. The row is kept because temperature and rainfall are still valid. |
| 5 | Exact duplicate rows | 8 rows | 0.2% | **Quarantine** | All exact duplicates — same values across every column. Quarantined with reason `EXACT_DUPLICATE` rather than silently dropped. |
| 6 | Orphan parcel IDs — `PARCEL_098` and `PARCEL_099` exist in readings but not in metadata | 40 rows | 1.2% | **Quarantine** | Without metadata (mill, crop type, sowing date) these rows cannot contribute to any analysis. Quarantined with reason `ORPHAN_NO_METADATA` for the operations team to investigate. |

### parcel_metadata.csv — 28 rows × 5 columns

| # | Issue | Count | % | Decision | Justification |
|---|-------|-------|---|----------|---------------|
| 7 | Parcels with no readings — `PARCEL_050`, `PARCEL_051`, `PARCEL_052` | 3 parcels | 11% | **Keep** | Parcels exist but have no sensor data in this extract. Inner join naturally excludes them. No rows fabricated. |

**No issues found with:** `temperature_c` (range 15–36°C, plausible), `rainfall_mm` (no negatives), `area_hectares` (no nulls, plausible range), `crop_type` (three clean values), `mill_id` (four clean values).


## Pipeline Decisions

### Decision 1 — Dead Letter / Quarantine Table Instead of Dropping

Most pipelines silently drop bad rows. This one does not.

Every rejected row — duplicates and orphans — lands in a separate Delta table called `quarantine_records` with a `quarantine_reason` and `quarantine_timestamp`. This means:

- Bad rows are **recoverable** if cleaning logic has a bug
- The operations team can **investigate** why PARCEL_098 and PARCEL_099 have no metadata
- There is a full **audit trail** — every one of the 3,447 raw rows is accounted for


Raw rows      : 3,447
Clean rows    : 3,399  → cleaned_parcel_timeseries (Delta)
Quarantined   :    48  → quarantine_records (Delta)
              -------
Accounted for : 3,447  nothing silently lost


### Decision 2 — UNKNOWN as a Third Sensor Status Category

It would have been easy to fill null `sensor_status` with `OK`. It would have been wrong.

137 rows have genuinely unknown sensor health — `NA`, `NaN`, `NULL`, and blank. Calling them `OK` inflates the pool of clean readings. Calling them `ERROR` discards potentially valid NDVI data. `UNKNOWN` is honest — analysis steps that filter `sensor_status = OK` safely exclude these rows without the pipeline making a guess that could corrupt results.

### Decision 3 — Null NDVI, Keep the Row

104 rows had NDVI outside `[-1, 1]`. The NDVI value was nulled rather than dropping the row entirely, because those rows still have valid temperature and rainfall readings. A weather analysis would want those rows. An NDVI analysis would skip them. Adding `ndvi_flag = true` makes it transparent why the value is missing.

### Decision 4 — Inner Join, Not Left Join

Three metadata parcels ('PARCEL_050', 'PARCEL_051', 'PARCEL_052') have no readings. A left join would keep them as rows of nulls — noise in every downstream aggregation. An inner join means every row in the output is complete and usable.

### Decision 5 — Delta Tables, Not CSV Output

Output is written as Delta tables rather than flat CSVs because:
- Delta supports schema enforcement — future ingestions with wrong column types fail loudly
- Delta supports time travel — if a cleaning decision was wrong, previous versions are recoverable
- Delta supports MERGE / upsert — when this pipeline runs incrementally, new rows can be merged without full rewrites
- In production, the Gold layer table is directly queryable by BI tools without an export step



## Analysis Output

Mean NDVI in the 30 days before and after `sowing_date`, using only `sensor_status = OK` rows:

| crop_type | mean_ndvi_before | mean_ndvi_after | n_parcels |
|-----------|-----------------|-----------------|-----------|
| soybean   | 0.1706          | 0.3024          | 4         |
| sugarcane | 0.1775          | 0.3242          | 19        |
| wheat     | 0.1761          | 0.3086          | 2         |

Interpretation:

All three crop types show a consistent NDVI lift of approximately 0.13 units in the 30 days following sowing — the expected green-up signal as seedlings emerge and canopy cover builds. Pre-sowing NDVI is remarkably similar across all three crops (~0.17), suggesting comparable baseline vegetation cover before planting begins.

Sugarcane shows the highest post-sowing NDVI (0.3242), consistent with its faster early canopy development. The consistency of the post-sowing lift across all crop types gives confidence that the cleaning pipeline preserved the real signal in the data.

The wheat result (n=2 parcels) should be treated with caution — two parcels is too small a sample to draw crop-level conclusions. In production a confidence interval would be required before including this number in any report or model feature.

---

## Production Reflection

### Three Things I Would Change at 100× Scale

1. Incremental processing with watermarking**

The current pipeline reprocesses all 3,447 rows on every run. At 340,000 rows with daily appends that is wasteful. Watermarking by `date` and using Delta `MERGE` to upsert into the Silver table cuts per-run cost from O(all data) to O(new data).

2. Declarative schema contracts instead of hardcoded checks**

The "ndvi_value < -1" check works today but won't survive a schema change or a new sensor type with a different valid range. Moving to Pandera or Great Expectations — declarative contracts that fail loudly and log to a data quality dashboard — means violations are visible immediately rather than silently producing nulls.

Par3.titioned Delta storage by date

At 100× scale, reading the entire dataset for each run becomes expensive. Partitioning by date (year/month) means each daily run reads only the relevant partition, not all historical data.

What I Would Monitor in Production

- Quarantine rate per run — a spike means something changed upstream: new sensor firmware, new date format, new parcel IDs not yet registered in metadata
- Row count per ingestion vs trailing 7-day average — a sudden drop signals a missed delivery; a spike signals a duplicate feed
- Null rate for "ndvi_value" per parcel per day — a sustained spike on one parcel means a sensor is failing
- Join hit-rate — growth in orphan records means device registration is lagging sensor deployment

#Most Likely Silent Failure

Date parsing breaking on a new format

`try_to_date` with three explicit format patterns is robust — but if a new upstream system sends dates as Unix timestamps or in US format (`05/16/2026` instead of `16/05/2026`), the parser returns NULL rather than raising an error. Those rows flow into the quarantine table silently. If nobody is monitoring the quarantine rate, the data loss goes unnoticed until someone questions anomalous seasonality in a downstream report — weeks later.

The fix is simple: a monitoring alert that pages the on-call engineer if the quarantine rate exceeds 2% of daily rows.

## Assumptions

| Assumption | Reasoning |
|------------|-----------|
| `dayfirst=True` for ambiguous dates | Data originates from a non-US region (sugarcane farming) so `16/05/2026` means 16th May not May 16th |
| NDVI valid range is strictly [-1, 1] | Per the assignment brief — any value outside this range is instrument error not a real reading |
| `sensor_status = UNKNOWN` excluded from analysis | Cannot confirm sensor was healthy — safer to exclude than include |
| Inner join on parcel_id | Every analysis row must have both readings and metadata — incomplete rows add noise |
| 30-day window means days -30 to -1 before, 0 to +29 after | Sowing date itself (day 0) counted as first day of post-sowing period |
| Orphan parcels are registration lag not decommissioned farms | They have recent active readings suggesting live sensors not retired ones |
| Duplicate rows with identical values are ingestion errors | No two sensors on same parcel should produce identical readings on same day |

---

## What I'd Improve With More Time

**1. Per-parcel NDVI anomaly detection**
Flag individual parcels whose post-sowing NDVI lift is significantly below
the crop-type average. If sugarcane average lift is +0.13 but PARCEL_015
only shows +0.02 — that farm needs a field inspection. This turns the
pipeline from descriptive to actionable.

**2. Weather correlation analysis**
Correlate `rainfall_mm` and `temperature_c` with NDVI change over the
following 7 days. This would tell Carnot whether irrigation interventions
are actually improving crop health — a direct business insight.

**3. Confidence intervals on the analysis output**
The current output is a single mean per crop type. With more time I'd add
standard deviation and a 95% confidence interval — especially important
for wheat where n=2 parcels makes the mean unreliable.

**4. Automated quarantine alerting**
Right now the quarantine table is written silently. With more time I'd add
a monitoring alert if quarantine rate exceeds 2% of daily rows — so the
operations team is notified about orphan parcels automatically rather than
waiting for someone to query the table.

**5. Historical baseline comparison**
With more seasons of data, compare this year's NDVI growth curve against
the historical average for each crop and region. A farmer could then see
not just "my crop is growing" but "my crop is growing 15% slower than
last year at this point in the season."

| Tool | Purpose |
|------|---------|
| Databricks Community Edition | Compute + notebook environment |
| Apache Spark (PySpark) | Distributed data processing |
| Delta Lake | Storage format — ACID, time travel, schema enforcement |
| Matplotlib | Data quality dashboard visualisation |
| GitHub | Version control |



## Repository Structure


Harsh-data-engineer-assignment/

│   00_data_audit.ipynb      ← full pipeline (Bronze → Silver → Gold)
|   Cleaned_parcel_timeseries.csv  ←  output file


---


How I Used AI Tools
Claude (claude.ai) was used to:

Draft the initial pipeline structure and README outline
Suggest try_to_date with coalesce for handling mixed date formats in PySpark.

*Built by Harshal — May 2026*
