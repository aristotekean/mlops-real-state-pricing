"""Form definition for the real estate inference contract.

The frontend talks to the API only, so it keeps its own copy of the field list
(see docs/data-contract.md). Option lists are a convenience; the API remains the
source of truth and rejects anything outside its contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

# Shown for categorical/text fields whose value can be missing; sent as JSON null.
UNKNOWN = "(desconocido)"

Kind = Literal["select", "number", "text"]


@dataclass(frozen=True)
class Field:
    name: str
    label: str
    kind: Kind
    options: tuple[str, ...] = ()
    nullable: bool = False
    min_value: float = 0.0
    max_value: float = 0.0
    default: Any = None
    # 0 => integer field; >0 => float field with this many decimals.
    decimals: int = 0
    help: str | None = None


def _select(name: str, label: str, options: tuple[str, ...], *, nullable: bool = False,
            default: str | None = None, help: str | None = None) -> Field:
    return Field(name, label, "select", options, nullable,
                 default=default if default is not None else options[0], help=help)


def _int(name: str, label: str, low: int, high: int, default: int,
         help: str | None = None) -> Field:
    return Field(name, label, "number", min_value=low, max_value=high,
                 default=default, decimals=0, help=help)


def _float(name: str, label: str, low: float, high: float, default: float,
           decimals: int = 2, help: str | None = None) -> Field:
    return Field(name, label, "number", min_value=low, max_value=high,
                 default=default, decimals=decimals, help=help)


def _text(name: str, label: str, *, default: str | None = None,
          help: str | None = None) -> Field:
    return Field(name, label, "text", nullable=True, default=default, help=help)


# Listing status observed in the source dataset. The target column `price` and
# the identifier-like `street` are NOT model inputs and never appear here.
STATUS_VALUES = ("for_sale", "ready_to_build", "sold")

# US states and territories (the source is US real estate listings).
US_STATES = (
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois",
    "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
    "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia",
    "Virgin Islands", "Washington", "West Virginia", "Wisconsin", "Wyoming",
)

SECTIONS: tuple[tuple[str, tuple[Field, ...]], ...] = (
    ("Propiedad", (
        _select("status", "Estado del listado", STATUS_VALUES,
                help="for_sale = en venta, ready_to_build = lista para construir, sold = vendida."),
        _int("bed", "Habitaciones", 0, 20, 3),
        _int("bath", "Baños", 0, 20, 2),
        _int("house_size", "Área habitable (sq ft)", 100, 30_000, 1_800),
        _float("acre_lot", "Tamaño del terreno (acres)", 0.0, 1_000.0, 0.25, decimals=2),
    )),
    ("Ubicación", (
        _text("city", "Ciudad", default="Austin"),
        _select("state", "Estado", US_STATES, nullable=True, default="Texas"),
        _text("zip_code", "Código postal (ZIP)", default="78701",
              help="5 dígitos, p. ej. 78701. Vacío = desconocido."),
    )),
    ("Metadatos", (
        _text("brokered_by", "Agencia/corredor (código)", default="101",
              help="Código numérico del corredor. Vacío = desconocido."),
        _text("prev_sold_date", "Fecha de venta anterior", default=None,
              help="Formato YYYY-MM-DD. Vacío = sin venta previa."),
    )),
)

FIELDS: tuple[Field, ...] = tuple(field for _, fields in SECTIONS for field in fields)
FIELDS_BY_NAME = {field.name: field for field in FIELDS}


def default_values() -> dict[str, Any]:
    return {field.name: field.default for field in FIELDS}


def _coerce_number(field: Field, value: Any) -> Any:
    return float(value) if field.decimals > 0 else int(value)


def to_widget_value(field: Field, value: Any) -> Any:
    """Map an API value (``None`` for missing) to what the widget displays."""
    if field.kind == "select":
        return UNKNOWN if value is None else value
    if field.kind == "text":
        return "" if value is None else str(value)
    return _coerce_number(field, value)


def from_widget_value(field: Field, value: Any) -> Any:
    """Map a widget value back to the JSON value the API expects."""
    if field.kind == "select":
        return None if value == UNKNOWN else value
    if field.kind == "text":
        text = str(value).strip()
        return text or None
    return _coerce_number(field, value)


def widget_options(field: Field) -> list[str]:
    options = list(field.options)
    return [*options, UNKNOWN] if field.nullable else options
