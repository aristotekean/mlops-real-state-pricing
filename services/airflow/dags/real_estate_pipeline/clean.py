"""CLEAN layer: typed, model-ready listings with lineage to RAW."""

from collections import Counter
from typing import Any

from sqlalchemy import text

from real_estate_model.contract import (
    SOURCE_COLUMNS,
    exclusion_reason,
    normalize_value,
    split_key,
)
from real_estate_model.splits import SPLIT_NAMES, SPLIT_SALT, assign_split
from real_estate_pipeline import history, settings
from real_estate_pipeline.db import copy_rows, engine_scope
from real_estate_pipeline.validation import _conformed

CLEAN_COLUMNS = (
    "raw_record_id", "batch_id", "transformation_version",
    "brokered_by", "status", "bed", "bath", "acre_lot", "street", "city", "state",
    "zip_code", "house_size", "prev_sold_date", "price",
    "exclusion_reason", "data_split",
)
_VALUE_COLUMNS = CLEAN_COLUMNS[3:]  # updated on conflict


def _process_batch(engine: Any, batch_id: str) -> dict[str, Any]:
    """Upsert one batch's validated RAW rows into CLEAN under CLEAN_VERSION.

    Upsert instead of delete+insert keeps `clean_record_id` stable when a batch
    is reprocessed, so lineage and snapshots recorded earlier stay valid. Rows
    of the batch that are no longer validated are removed.
    """
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT raw_record_id, record_data FROM raw.records
                WHERE batch_id = :id AND status = 'validated' ORDER BY row_number
                """
            ),
            {"id": batch_id},
        ).all()

    excluded: Counter[str] = Counter()
    splits: Counter[str] = Counter()
    values = []
    for raw_record_id, data in rows:
        record = _conformed(data)
        reason = exclusion_reason(record, settings.TRAINING_BOUNDS)
        split = None if reason else assign_split(split_key(record))
        if reason:
            excluded.update(reason.split(";"))
        else:
            splits[split] += 1
        normalized = {column: normalize_value(column, record[column]) for column in SOURCE_COLUMNS}
        values.append(
            (
                raw_record_id, batch_id, settings.CLEAN_VERSION,
                *(normalized[column] for column in CLEAN_COLUMNS[3:15]),
                reason, split,
            )
        )

    columns = ", ".join(CLEAN_COLUMNS)
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in _VALUE_COLUMNS)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TEMP TABLE tmp_clean ON COMMIT DROP AS "
                f"SELECT {columns} FROM clean.records WITH NO DATA"
            )
        )
        copy_rows(connection, "tmp_clean", CLEAN_COLUMNS, values)
        connection.execute(
            text(
                f"""
                INSERT INTO clean.records ({columns})
                SELECT {columns} FROM tmp_clean
                ON CONFLICT (raw_record_id, transformation_version) DO UPDATE
                SET {updates}, processed_at = CURRENT_TIMESTAMP
                """
            )
        )
        connection.execute(
            text(
                """
                DELETE FROM clean.records c
                WHERE c.batch_id = :id AND c.transformation_version = :v
                  AND NOT EXISTS (SELECT 1 FROM tmp_clean t WHERE t.raw_record_id = c.raw_record_id)
                """
            ),
            {"id": batch_id, "v": settings.CLEAN_VERSION},
        )
    return {"rows": len(values), "splits": splits, "excluded": excluded}


def preprocess_data(batch: dict[str, Any]) -> dict[str, Any]:
    """Transform the batch's validated RAW rows into clean.records.

    * values normalised to the contract types (codes as text, ZIP codes with
      their leading zeros, numbers as double, dates as date);
    * rows outside TRAINING_BOUNDS keep an ``exclusion_reason`` and no split;
    * eligible rows get a deterministic, property-level split (salted hash of
      street+city+state+zip), so a property never straddles train and test and
      earlier assignments never change when new batches arrive.

    Earlier batches missing under the current CLEAN_VERSION (the training
    bounds changed) are rebuilt first, so CLEAN is always consistent.
    Idempotent: re-running upserts the same rows.
    """
    batch_id = batch["batch_id"]
    with engine_scope() as engine:
        with engine.connect() as connection:
            backfill = connection.execute(
                text(
                    """
                    SELECT b.batch_id::text FROM raw.batches b
                    WHERE b.batch_id <> :id AND b.status = 'stored'
                      AND EXISTS (SELECT 1 FROM raw.records r
                                  WHERE r.batch_id = b.batch_id AND r.status = 'validated')
                      AND NOT EXISTS (SELECT 1 FROM clean.records c
                                      WHERE c.batch_id = b.batch_id AND c.transformation_version = :v)
                    ORDER BY b.received_at
                    """
                ),
                {"id": batch_id, "v": settings.CLEAN_VERSION},
            ).scalars().all()
        for other in backfill:
            stats = _process_batch(engine, other)
            print(f"Rebuilt earlier batch {other} under {settings.CLEAN_VERSION}: {stats['rows']} rows")

        stats = _process_batch(engine, batch_id)
        with engine.connect() as connection:
            totals = connection.execute(
                text(
                    """
                    SELECT data_split, COUNT(*) FROM clean.records
                    WHERE transformation_version = :v AND data_split IS NOT NULL
                    GROUP BY data_split
                    """
                ),
                {"v": settings.CLEAN_VERSION},
            ).all()

    splits, excluded = stats["splits"], stats["excluded"]
    accumulated = {split: 0 for split in SPLIT_NAMES} | dict(totals)
    report = {
        "transformation_version": settings.CLEAN_VERSION,
        "split_salt": SPLIT_SALT,
        "clean_rows": stats["rows"],
        "eligible_rows": sum(splits.values()),
        "excluded_rows": stats["rows"] - sum(splits.values()),
        "exclusion_reasons": dict(excluded),
        "batch_split_rows": {split: splits.get(split, 0) for split in SPLIT_NAMES},
        "accumulated_split_rows": accumulated,
        "accumulated_eligible_rows": sum(accumulated.values()),
        "rebuilt_batches": list(backfill),
    }
    history.update(batch_id, preprocessing_report=report)
    print(
        f"CLEAN {settings.CLEAN_VERSION}: {report['clean_rows']} rows ({report['eligible_rows']} eligible, "
        f"{report['excluded_rows']} excluded {dict(excluded)}); accumulated {accumulated}; "
        f"rebuilt {len(backfill)} earlier batches"
    )
    return {
        "eligible_rows": report["eligible_rows"],
        "accumulated_split_rows": accumulated,
    }
