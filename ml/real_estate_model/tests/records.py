"""Source records for the tests, shaped like rows of the listings CSV."""

from typing import Any

CITIES = (
    ("Adjuntas", "Puerto Rico", "601"),
    ("Springfield", "Illinois", "62701"),
    ("Springfield", "Massachusetts", "1101"),
    ("Austin", "Texas", "73301"),
)


def source_record(**overrides: Any) -> dict[str, Any]:
    """One valid record, as strings, the way the CSV is read."""
    record: dict[str, Any] = {
        "brokered_by": "103378.0",
        "status": "for_sale",
        "price": "105000.0",
        "bed": "3.0",
        "bath": "2.0",
        "acre_lot": "0.12",
        "street": "1962661.0",
        "city": "Adjuntas",
        "state": "Puerto Rico",
        "zip_code": "601.0",
        "house_size": "920.0",
        "prev_sold_date": "2020-07-01",
    }
    record.update(overrides)
    return record


def source_records(count: int) -> list[dict[str, Any]]:
    """Distinct valid records whose price follows size, bedrooms and city."""
    records = []
    for index in range(count):
        city, state, zip_code = CITIES[index % len(CITIES)]
        beds = 1 + index % 5
        size = 600 + 37 * (index % 40)
        price = 40_000 + 150 * size + 20_000 * beds + 30_000 * (index % len(CITIES))
        records.append(
            source_record(
                brokered_by=str(1000 + index % 7),
                status="for_sale" if index % 3 else "sold",
                price=str(float(price)),
                bed=str(beds),
                bath=str(1 + index % 3),
                acre_lot=str(round(0.05 + 0.01 * (index % 25), 2)),
                street=str(500_000 + index),
                city=city,
                state=state,
                zip_code=zip_code,
                house_size=str(size),
                prev_sold_date="" if index % 4 == 0 else f"{2000 + index % 20}-03-15",
            )
        )
    return records
