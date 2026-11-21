"""Deterministic, property-disjoint train/validation/test assignment."""

import hashlib

SPLIT_SALT = "real-estate-property-split-v1"
TRAIN_FRACTION = 0.70
VALIDATION_FRACTION = 0.15
TEST_FRACTION = 0.15
SPLIT_NAMES = ("train", "validation", "test")


def assign_split(key: str) -> str:
    """Assign a split from a salted hash of ``contract.split_key(record)``.

    The assignment depends only on the key, so every listing of a property
    lands in the same partition regardless of the batch or order it arrives in,
    and earlier assignments never change as new batches are loaded.
    """
    if key is None or not str(key).strip():
        raise ValueError("A split key is required to assign a split.")
    digest = hashlib.sha256(f"{SPLIT_SALT}:{str(key).strip()}".encode()).digest()
    position = int.from_bytes(digest[:8], "big") / 2**64
    if position < TRAIN_FRACTION:
        return "train"
    if position < TRAIN_FRACTION + VALIDATION_FRACTION:
        return "validation"
    return "test"
