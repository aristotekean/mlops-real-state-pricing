"""Database helpers shared by the tasks: engine lifecycle and bulk COPY."""

import csv
import io
import json
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from real_estate_pipeline.settings import database_engine


@contextmanager
def engine_scope() -> Iterator[Any]:
    """One engine per task call, always disposed (tasks run in short processes)."""
    engine = database_engine()
    try:
        yield engine
    finally:
        engine.dispose()


def _csv_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    return value


def copy_rows(
    connection: Any, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]]
) -> None:
    """Bulk-load rows with PostgreSQL COPY on an open SQLAlchemy connection.

    Hundreds of thousands of rows per batch make row-by-row INSERTs far too
    slow; COPY loads them in seconds. QUOTE_STRINGS (Python >= 3.12) quotes
    every string and writes ``None`` as an unquoted empty field, which COPY's
    CSV mode reads as NULL while a quoted "" stays an empty string.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_STRINGS, lineterminator="\n")
    for row in rows:
        writer.writerow([_csv_value(value) for value in row])
    buffer.seek(0)
    sql = (
        f"COPY {table} ({', '.join(columns)}) FROM STDIN "
        "WITH (FORMAT csv, NULL '')"
    )
    cursor = connection.connection.dbapi_connection.cursor()
    try:
        cursor.copy_expert(sql, buffer)
    finally:
        cursor.close()
