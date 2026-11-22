"""Example listings for the "load example" buttons.

Plausible rows of the US real estate dataset restricted to the model input
contract (the target ``price`` and the excluded ``street`` are removed).
``observed`` is a reference price shown only as a hint, not a ground truth.
"""

from __future__ import annotations

from typing import Any

EXAMPLES: dict[str, dict[str, Any]] = {
    "Casa familiar · Austin, TX · 1.800 sq ft": {
        "observed": 465_000.0,
        "features": {
            "status": "for_sale",
            "bed": 3,
            "bath": 2,
            "house_size": 1_800.0,
            "acre_lot": 0.25,
            "city": "Austin",
            "state": "Texas",
            "zip_code": "78701",
            "brokered_by": "101",
            "prev_sold_date": "2018-06-15",
        },
    },
    "Apartamento · Nueva York, NY · sin terreno": {
        "observed": 980_000.0,
        "features": {
            "status": "for_sale",
            "bed": 2,
            "bath": 1,
            "house_size": 950.0,
            "acre_lot": 0.0,
            "city": "New York",
            "state": "New York",
            "zip_code": "10001",
            "brokered_by": "532",
            "prev_sold_date": None,
        },
    },
    "Lote para construir · Phoenix, AZ": {
        "observed": 120_000.0,
        "features": {
            "status": "ready_to_build",
            "bed": 0,
            "bath": 0,
            "house_size": 100.0,
            "acre_lot": 1.10,
            "city": "Phoenix",
            "state": "Arizona",
            "zip_code": "85001",
            "brokered_by": "77",
            "prev_sold_date": None,
        },
    },
}
