"""Load test for the Inference API: simulated users scoring listings on /predict.

Run with the compose profile `loadtest` and open the UI on http://<host>:8089.
Every request carries the model input contract of docs/data-contract.md, built
from plausible value ranges of the US real estate dataset, so the API does the
same work as with real traffic (validation, model scoring and one
inference.logs row).

Finding the degradation point: set LOCUST_STEP_LOAD=1 and the run follows a
staircase instead of the users/spawn rate typed in the UI. Each step adds
LOCUST_STEP_USERS users and holds for LOCUST_STEP_SECONDS, up to
LOCUST_STEP_MAX_USERS; the step at which p95 latency or failures jump in Grafana
or in the Locust charts is the approximate limit for the configured resources.
"""

import os
import random
from datetime import date, timedelta

from locust import FastHttpUser, LoadTestShape, between, task

STATUS_VALUES = ["for_sale", "ready_to_build", "sold"]
STATUS_WEIGHTS = [85, 8, 7]

# A spread of US cities paired with a plausible state (keeps the pair coherent).
CITY_STATE = [
    ("New York", "New York"), ("Los Angeles", "California"), ("Chicago", "Illinois"),
    ("Houston", "Texas"), ("Phoenix", "Arizona"), ("Philadelphia", "Pennsylvania"),
    ("San Antonio", "Texas"), ("San Diego", "California"), ("Dallas", "Texas"),
    ("Austin", "Texas"), ("Miami", "Florida"), ("Seattle", "Washington"),
    ("Denver", "Colorado"), ("Boston", "Massachusetts"), ("Atlanta", "Georgia"),
    ("San Juan", "Puerto Rico"),
]


def _random_date_or_none() -> str | None:
    """A prior-sale date within the last ~30 years, or None (never sold)."""
    if random.random() < 0.35:
        return None
    start = date(1995, 1, 1)
    offset = random.randint(0, (date(2024, 1, 1) - start).days)
    return (start + timedelta(days=offset)).isoformat()


def random_listing() -> dict:
    """One property inside the ranges and categories of the training data."""
    city, state = random.choice(CITY_STATE)
    beds = random.choices([0, 1, 2, 3, 4, 5, 6], [3, 8, 20, 32, 22, 10, 5])[0]
    status = random.choices(STATUS_VALUES, STATUS_WEIGHTS)[0]
    # A lot without a house (ready_to_build) has no living area.
    house_size = 100 if status == "ready_to_build" else max(200, min(10_000, int(random.gauss(1_900, 900))))
    return {
        "status": status,
        "bed": beds,
        "bath": max(0, min(beds + 1, int(random.gauss(beds, 1)))),
        "house_size": house_size,
        "acre_lot": round(min(50.0, random.expovariate(1 / 0.5)), 2),
        "city": random.choice([city, None]) if random.random() < 0.05 else city,
        "state": state,
        "zip_code": f"{random.randint(1, 99999):05d}",
        "brokered_by": str(random.randint(1, 1200)),
        "prev_sold_date": _random_date_or_none(),
    }


class InferenceUser(FastHttpUser):
    """A client application scoring one listing at a time."""

    wait_time = between(0.5, 1.5)

    @task(20)
    def predict(self) -> None:
        with self.client.post(
            "/predict", json={"features": random_listing()}, name="/predict",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f"HTTP {response.status_code}")
                return
            body = response.json()
            if "prediction" not in body or "model" not in body:
                response.failure("response without prediction/model")

    @task(1)
    def model_info(self) -> None:
        self.client.get("/model-info")


if os.environ.get("LOCUST_STEP_LOAD") == "1":

    class StepLoadShape(LoadTestShape):
        """Staircase: +step_users every step_seconds until max_users, then stop."""

        step_users = int(os.environ.get("LOCUST_STEP_USERS", "10"))
        step_seconds = int(os.environ.get("LOCUST_STEP_SECONDS", "60"))
        max_users = int(os.environ.get("LOCUST_STEP_MAX_USERS", "100"))

        def tick(self):
            step = int(self.get_run_time() // self.step_seconds) + 1
            users = step * self.step_users
            if users > self.max_users:
                return None
            return users, self.step_users
