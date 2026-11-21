import pytest
from records import source_record

from real_estate_model.contract import (
    INPUT_COLUMNS,
    NUMERIC_INPUTS,
    SOURCE_COLUMNS,
    input_frame,
    prepare_input,
    record_key,
    split_key,
    validate_source_record,
)
from real_estate_model.splits import SPLIT_NAMES, assign_split


def test_valid_record_has_no_violations():
    assert validate_source_record(source_record()) == []


def test_optional_fields_may_be_missing():
    record = source_record(
        brokered_by="", bed="", bath="", acre_lot="", street="", city="",
        state="", zip_code="", house_size="", prev_sold_date="",
    )
    assert validate_source_record(record) == []


def test_missing_column_is_reported_alone():
    record = source_record()
    del record["house_size"]
    assert validate_source_record(record) == ["missing_columns:house_size"]


@pytest.mark.parametrize(
    ("overrides", "violation"),
    [
        ({"price": ""}, "missing_price"),
        ({"price": "0"}, "non_positive_price"),
        ({"price": "-1"}, "negative_price"),
        ({"price": "cheap"}, "invalid_price"),
        ({"price": "inf"}, "invalid_price"),
        ({"status": ""}, "missing_status"),
        ({"bed": "2.5"}, "invalid_bed"),
        ({"bath": "two"}, "invalid_bath"),
        ({"acre_lot": "-0.5"}, "negative_acre_lot"),
        ({"house_size": "big"}, "invalid_house_size"),
        ({"prev_sold_date": "07/01/2020"}, "invalid_prev_sold_date"),
    ],
)
def test_violations(overrides, violation):
    assert validate_source_record(source_record(**overrides)) == [violation]


def test_input_contract_excludes_target_and_street():
    assert "price" not in INPUT_COLUMNS
    assert "street" not in INPUT_COLUMNS
    assert set(INPUT_COLUMNS) == set(SOURCE_COLUMNS) - {"price", "street"}
    assert NUMERIC_INPUTS == ("bed", "bath", "acre_lot", "house_size")


def test_prepare_input_types_every_field():
    assert prepare_input(source_record()) == {
        "brokered_by": "103378",
        "status": "for_sale",
        "bed": 3.0,
        "bath": 2.0,
        "acre_lot": 0.12,
        "city": "Adjuntas",
        "state": "Puerto Rico",
        "zip_code": "00601",
        "house_size": 920.0,
        "prev_sold_date": "2020-07-01",
    }


def test_prepare_input_maps_missing_to_none_and_rejects_bad_numbers():
    prepared = prepare_input(source_record(bed="", zip_code="nan", prev_sold_date=None))
    assert (prepared["bed"], prepared["zip_code"], prepared["prev_sold_date"]) == (
        None,
        None,
        None,
    )
    with pytest.raises(ValueError, match="house_size must be numeric"):
        prepare_input(source_record(house_size="big"))


def test_input_frame_dtypes():
    frame = input_frame([source_record(), source_record(bed="", city="")])
    assert list(frame.columns) == list(INPUT_COLUMNS)
    assert all(frame[column].dtype == "float64" for column in NUMERIC_INPUTS)
    assert frame["zip_code"].tolist() == ["00601", "00601"]
    assert frame["city"].tolist() == ["Adjuntas", None]


def test_record_key_identifies_content():
    assert record_key(source_record()) == record_key(source_record())
    assert record_key(source_record()) != record_key(source_record(price="1.0"))
    assert len(record_key(source_record())) == 64


def test_split_key_groups_listings_of_one_property():
    relisted = source_record(price="250000.0", status="sold")
    assert split_key(source_record()) == split_key(relisted)
    assert split_key(source_record()) != split_key(source_record(street="7.0"))
    # Without an address every record stands on its own.
    assert split_key(source_record(street="")) != split_key(
        source_record(street="", price="1.0")
    )


def test_assign_split_is_deterministic_and_roughly_70_15_15():
    keys = [f"property:{index}" for index in range(4000)]
    splits = [assign_split(key) for key in keys]
    assert splits == [assign_split(key) for key in keys]
    assert set(splits) == set(SPLIT_NAMES)
    assert 0.67 < splits.count("train") / len(splits) < 0.73
    with pytest.raises(ValueError):
        assign_split(" ")
