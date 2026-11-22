"""Make the DAG folder importable: the pipeline package lives in dags/."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
