# Data contract

This contract defines the PostgreSQL boundaries between batch ingestion,
training, and inference. The source is the course Data API (group 6), which
serves US real estate listings in six batches of 12 columns each. The source
has no row or property identifier.

The rules are implemented once, in the shared package
[`ml/real_estate_model`](../ml/real_estate_model), and reused by the Airflow
DAG, the registered model, and the inference API. The pipeline that applies
them (tasks, training rules, promotion rule, thresholds) is described in
[pipeline.md](pipeline.md).

## Modeling decisions

- **Target:** `price`, the listing price in USD. It is a regression problem.
  Every candidate is trained on `log1p(price)` and predicts in USD.
- **Selection and promotion metric:** validation **MAE** (`PRIMARY_METRIC`),
  with **RMSE** as the guard metric (`GUARD_METRIC`). RMSLE, median absolute
  error, MAPE, and R² are logged as supporting metrics for train, validation,
  and test.
- **Production alias:** `production` on the registered model `real_estate`.

## Source schema (`real-estate-listings-v1`)

| Column | Type | Meaning | Model input |
|---|---|---|---|
| `brokered_by` | code | Encoded broker or agency | yes |
| `status` | category | Listing status, for example `for_sale` or `sold` | yes |
| `price` | float | Listing price in USD | no (target) |
| `bed` | integer | Bedrooms | yes |
| `bath` | integer | Bathrooms | yes |
| `acre_lot` | float | Lot size in acres | yes |
| `street` | code | Encoded street address | no (lineage and splits only) |
| `city` | category | City | yes |
| `state` | category | State or territory | yes |
| `zip_code` | code | Postal code | yes |
| `house_size` | float | Living area in square feet | yes |
| `prev_sold_date` | date | Previous sale date, `YYYY-MM-DD` | yes |

Types are logical types, declared in `contract.COLUMN_TYPES`:

- **code:** a categorical label that looks like a number. It is kept as text,
  a spurious `.0` suffix is dropped, and `zip_code` is left-padded to five
  digits.
- **category:** categorical text.
- **integer / float:** non-negative numbers, stored as `double`.
- **date:** ISO 8601 calendar date.

The missing tokens are the empty string, `nan`, `null`, and `none`
(case-insensitive); they become null. No column has a closed domain
(`ALLOWED_VALUES` is empty), so new categories are valid data.

## PostgreSQL schemas and migrations

The migrations in [`infra/database/migrations`](../infra/database/migrations)
create four schemas in the training database (`postgres-train`):

- `raw`: every request to the Data API, every response as received, and one
  row per listing with its validation status.
- `clean`: typed listings ready for training, with lineage to RAW, exclusion
  reason, and split.
- `audit`: one row per batch with every report, decision, and MLflow
  identifier.
- `inference`: one auditable event per inference request, plus its feedback.

[`infra/database/migrate.sh`](../infra/database/migrate.sh) applies pending
files in name order, each in its own transaction, and records them in
`public.schema_migrations`. Docker Compose runs it as the one-shot
`train-db-migrate` service and Kubernetes as the Job of the same name. It uses
the standard libpq variables (`PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`).
Every migration is also written to be safe to re-run.

## RAW layer

Each DAG run asks the Data API for **one** batch. The API is stateful: every
call hands out the next batch and a lost response cannot be requested again,
so the response body is persisted before anything else happens.

`raw.api_requests` (one row per fetch attempt, whatever its outcome):

- `dag_run_id`, `requested_at`, `request_url`, `group_number`, `http_status`,
  `duration_ms`.
- `api_called`: `false` when the run resumed an unfinished batch instead of
  calling the API.
- `outcome`: `new`, `resumed`, `duplicate`, `empty`, `end_of_data`, or `error`.
- `api_batch_number`, `batch_id`, `response_sha256`, `record_count`, `detail`.

`raw.batches` (one row per response that carried data):

- `batch_id`: deterministic UUIDv5 of the group number, the batch number, and
  the response SHA-256.
- `group_number`, `api_batch_number`, `request_url`, `http_status`.
- `response_sha256`, `response_bytes`, `payload_gzip`: the response body,
  byte for byte.
- `record_count` and `received_schema` (column to observed JSON types).
- `status`: `landed` (body persisted), `stored` (rows written to
  `raw.records`), or `failed`.
- `pipeline_status`: `in_progress`, `completed`, or `failed`, for the whole
  DAG run of this batch.
- `dag_run_id`, `received_at`, `stored_at`, `completed_at`, `error_message`.

`raw.records` (one row per listing):

- `raw_record_id`: surrogate key used as CLEAN lineage.
- `batch_id` and `row_number` (one-based position in the batch).
- `record_key`: SHA-256 of the 12 source values, which identifies a listing
  and finds exact duplicates.
- `record_data`: the complete, unmodified listing as JSON.
- `status`: `loaded` on insert, then `validated` or `rejected` by the quality
  task; `status_reason` lists the violated rules; `validated_at`.

Idempotency: `(group_number, api_batch_number, response_sha256)` is unique in
`raw.batches`, so the same response received twice is recognised as
`duplicate` and never reprocessed. `(batch_id, row_number)` is unique in
`raw.records`. A batch that is not `completed` is resumed by the next run
before the API is called again.

### RAW quality rules

`validate_source_record` checks every record and returns its violations:

| Reason | Rule |
|---|---|
| `missing_columns:<list>` | One of the 12 source columns is absent |
| `missing_price`, `missing_status` | The target or a required column is missing |
| `invalid_<column>` | The value cannot be read as its type, or `bed` / `bath` is not a whole number |
| `negative_<column>` | A numeric value is below zero |
| `non_positive_price` | The price is zero |

Every other column may be missing. The quality task adds two reasons:
`duplicate_in_batch` for a repeated `record_key` inside the batch and
`duplicate_of_previous_batch` for a listing already validated in an earlier
batch.

A rejected row stays in RAW and never reaches CLEAN. A batch is **blocking**,
which means it is stored but does not train, when it lacks the `price` or
`status` column, has no rows, or has more than `MAX_REJECTED_FRACTION` (5%)
rejected rows. The DAG does not fail on bad data.

## CLEAN layer (`price-features-v1`)

Validated RAW records are normalised to the input contract and upserted into
`clean.records`.

`clean.records`:

- `clean_record_id`, `raw_record_id` and `batch_id` (lineage).
- `transformation_version`: `price-features-v1+bounds-<hash>`, the feature
  code version plus a hash of the training bounds below.
  `(raw_record_id, transformation_version)` is unique, so reprocessing is
  idempotent and versions coexist. When the bounds change, earlier batches are
  rebuilt under the new version before training continues.
- The ten input columns already typed: `brokered_by`, `status`, `city`,
  `state`, `zip_code` as text; `bed`, `bath`, `acre_lot`, `house_size` as
  `double precision`; `prev_sold_date` as `date`.
- `street`: kept for lineage and splits, never a model input.
- `price`: the target, always positive.
- `exclusion_reason`: set for valid listings that cannot be used for training.
- `data_split`: `train`, `validation`, or `test`. A row has a split exactly
  when it has no exclusion reason.
- `processed_at`.

CLEAN stores inputs, not engineered features: feature engineering runs inside
the model, so training and inference share it.

### Excluded listings

A valid listing outside the plausible training range stays in RAW and CLEAN
with `exclusion_reason = 'out_of_range_<column>'` (several reasons are joined
by `;`) and is not used for training. The source contains extremes such as a
515 M USD price, 444 bedrooms, 100,000 acres, and 1.5 M square feet, which
dominate any squared error.

| Column | Low | High |
|---|---:|---:|
| `price` | 10,000 | 20,000,000 |
| `bed` | none | 20 |
| `bath` | none | 20 |
| `house_size` | 100 | 30,000 |
| `acre_lot` | none | 1,000 |

These are the defaults of `DEFAULT_TRAINING_BOUNDS`; the pipeline can override
them with `TRAINING_BOUNDS_JSON`. Missing values are not out of range. With the
defaults about 0.1% of the rows are excluded.

### Inputs and features

The **input contract** has 10 fields: the 12 source columns minus `price` (the
target) and `street` (close to unique per property, so it identifies a listing
instead of describing it).

`engineer_features` turns those inputs into **11 model features**:

| Feature | Rule |
|---|---|
| `bed`, `bath`, `acre_lot`, `house_size` | Unchanged numeric inputs |
| `prev_sold_year` | Previous sale as a fractional year, for example `2020-07-01` becomes 2020.5. It is a point in time, so it does not depend on the day the pipeline or the API runs |
| `previously_sold` | 1 when `prev_sold_date` is present, otherwise 0 |
| `status`, `state` | Unchanged low-cardinality categories |
| `city_state` | `"<city>, <state>"`, because the same city name repeats across states |
| `zip_code`, `brokered_by` | Unchanged high-cardinality codes |

Fitted transformations happen inside the model pipeline, using the training
partition only:

- **Numeric:** median imputation, plus `log1p` and standard scaling for Ridge.
  Gradient boosting receives missing values as they are.
- **`status`, `state`:** nulls become `__missing__`, then one-hot encoding
  that ignores unseen categories. Gradient boosting uses ordinal encoding
  instead, where an unseen category becomes -1 and is treated as missing.
- **`city_state`, `zip_code`, `brokered_by`:** nulls become `__missing__`,
  then cross-fitted target encoding on the log price. An unseen category gets
  the global mean.

### Splits

`assign_split` hashes the property key (SHA-256 with the salt
`real-estate-property-split-v1`): 70% train, 15% validation, 15% test. The
property key is the address (`street`, `city`, `state`, `zip_code`); a listing
without a street uses its own `record_key`. The split depends only on the key,
so every listing of a property shares a partition and earlier assignments
never change when new batches arrive.

## AUDIT layer

`audit.batch_history` holds one row per batch. Each DAG task fills its own
columns as it finishes, so a failed run still shows how far the batch got:

- `schema_report`, `quality_report`, `new_categories_report`, `drift_report`,
  `preprocessing_report`.
- `training_decision` (`train` or `skip`), `training_reason`, `training_rules`.
- `mlflow_experiment_id`, `mlflow_parent_run_id`, `mlflow_candidate_run_id`,
  `candidate_model`, `candidate_version`, `candidate_metrics`.
- `production_version_before`, `production_metrics`, `metric_deltas`,
  `promotion_decision` (`promoted` or `rejected`), `promotion_reason`,
  `production_version_after`.
- `final_status` (`in_progress`, `completed`, `failed`) and `error_message`.

The view `audit.batch_history_summary` exposes one readable line per batch. The
inference API serves it at `GET /history` for the frontend.

## Training, registration, and promotion

A batch trains only when the training rules of [pipeline.md](pipeline.md)
say so. The candidates are Ridge and histogram gradient boosting
(`TRAINING_CANDIDATES`; random forest is available but not trained by
default). Each one is a nested MLflow run under the parent run of the DAG run
and logs parameters, train / validation / test metrics, plots, permutation
importance, the CLEAN snapshot, and the model.

The logged model is the **serving pipeline**: the stateless `FeatureEngineer`
step followed by the fitted preprocessing and regressor. It accepts the 10
input fields, has an explicit signature (numeric fields `double`, the rest
`string`, all optional; output `price` as `double`), bundles the
`real_estate_model` package through `code_paths`, and is serialized with
skops, trusting only an explicit list of types. A prediction is clipped in log
space so an absurd input cannot produce an infinite price.

The candidate with the best validation MAE is registered as a new version of
`real_estate`, tagged with the batches used and its CLEAN snapshot
fingerprint. A training set is reproducible from the CLEAN version and the
batch ids, which the snapshot records. The registered version and the current
`production` version are then loaded from the registry and scored on the same
accumulated validation rows. The new version must reproduce its training-time
score (a training/serving skew check). It takes the `production` alias only if
MAE improves by at least `PROMOTION_MIN_IMPROVEMENT` (3%) and RMSE does not get
worse by more than `PROMOTION_MAX_GUARD_DEGRADATION` (1%). With no production
model the candidate is promoted as the baseline. The test partition is
reported, never used to select.

## Inference API and INFERENCE LOGS

The API accepts one listing as JSON. `features` contains the 10 input fields
and all of them must be present: the four numeric fields are JSON numbers or
`null`; the other six are strings or `null`, including numeric-looking codes
and the date. Types are strict, so `"3"` is rejected for `bed` and `78701` for
`zip_code`. `price`, `street`, and any unknown key are rejected. The API loads
`models:/real_estate@production`, whose signature is authoritative.

Request:

```json
{
  "request_id": "optional UUID; the API generates one if omitted",
  "features": {
    "brokered_by": "101",
    "status": "for_sale",
    "bed": 3,
    "bath": 2,
    "acre_lot": 0.25,
    "city": "Austin",
    "state": "Texas",
    "zip_code": "78701",
    "house_size": 1800.0,
    "prev_sold_date": "2018-06-15"
  }
}
```

Successful response:

```json
{
  "request_id": "UUID",
  "prediction": 465000.0,
  "model": {
    "name": "real_estate",
    "version": "registered model version",
    "alias": "production"
  },
  "processing_time_ms": 12.5
}
```

`prediction` is the estimated price in USD. Errors: `422` (invalid body;
`error_code = validation_error`), `503` `model_unavailable` (no model with the
`production` alias is loaded), `500` `inference_error` (the model failed to
score the listing).

The API persists one `inference.logs` row for every request, including failed
requests. `input_data` is the exact submitted feature object; `prediction`,
model identity, and duration are populated on success. `status`, `error_code`,
and `error_message` describe failures. The `probabilities` column is unused by
this regression model and stays null. The `request_id` is the idempotency and
audit key and is returned to the caller. While the database is unavailable the
rows are spooled to disk and replayed later.

`trace_id` (`005_inference_logs_trace_id.sql`) links the row to its distributed
trace in Jaeger: the W3C trace id of the request, 32 lowercase hex characters.
It is set only when the trace was sampled, so a non-null value always points to
a trace that was exported. It stays null when tracing is off, when the request
was not sampled, and for rows written before the migration. Traces themselves
are not stored in PostgreSQL; see
[`infra/monitoring`](../infra/monitoring/README.md#trazas-jaeger).

### Feedback (ground truth)

`POST /predict/{request_id}/feedback` attaches the observed price to a logged
request:

```json
{ "price": 472500.0, "source": "MLS" }
```

`price` must be greater than zero; `source` is an optional free-text origin
(max 200 characters). One label per `request_id`: a later call replaces the
previous label. Response `200`:

```json
{ "request_id": "UUID", "price": 472500.0, "prediction": 465000.0, "created": true }
```

`prediction` is `null` when the logged request failed, and `created` is
`false` when a previous label was replaced. Errors: `404` `unknown_request`
(no `inference.logs` row), `422` (invalid body), `503` `database_unavailable`
(the caller must retry; feedback is never buffered).

Rows land in `inference.feedback` (`003_inference_feedback.sql`). The view
`inference.training_examples` joins every succeeded `inference.logs` row with
its label and exposes `features` (the submitted inputs), `predicted_price`,
and `price`. The inputs use the same ten fields as the typed columns of
`clean.records`, so a retraining pipeline can union both sources.
