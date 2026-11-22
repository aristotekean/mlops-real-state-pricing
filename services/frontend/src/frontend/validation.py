"""Client-side checks run before calling the API.

They catch obvious typos early; the API still validates every request and its
422 errors are shown as well.
"""

from __future__ import annotations

import re
from typing import Any

from frontend.fields import FIELDS

_ZIP = re.compile(r"^\d{5}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_features(features: dict[str, Any]) -> list[tuple[str, str]]:
    errors: list[tuple[str, str]] = []
    for field in FIELDS:
        value = features.get(field.name)
        if field.kind == "number":
            if value is None or not (field.min_value <= value <= field.max_value):
                errors.append(
                    (field.name, f"debe estar entre {field.min_value:g} y {field.max_value:g}")
                )
        elif field.kind == "select" and value is None and not field.nullable:
            errors.append((field.name, "es obligatorio"))
        elif field.kind == "text" and value is not None:
            if field.name == "zip_code" and not _ZIP.match(value):
                errors.append((field.name, "debe ser un código postal de 5 dígitos"))
            elif field.name == "prev_sold_date" and not _DATE.match(value):
                errors.append((field.name, "debe tener formato YYYY-MM-DD"))
            elif field.name == "brokered_by" and not value.isdigit():
                errors.append((field.name, "debe ser un código numérico"))
    return errors
