from frontend.examples import EXAMPLES
from frontend.fields import FIELDS, UNKNOWN, FIELDS_BY_NAME, from_widget_value, to_widget_value
from frontend.validation import validate_features

# The model input contract (docs/data-contract.md): the source columns minus the
# target (`price`) and the identifier-like `street`.
CONTRACT_INPUTS = {
    "brokered_by", "status", "bed", "bath", "acre_lot",
    "city", "state", "zip_code", "house_size", "prev_sold_date",
}
EXCLUDED = {"price", "street"}


def test_form_covers_the_contract_inputs():
    names = [field.name for field in FIELDS]
    assert len(names) == len(CONTRACT_INPUTS)
    assert set(names) == CONTRACT_INPUTS
    assert not EXCLUDED & set(names)


def test_defaults_and_options_are_consistent():
    for field in FIELDS:
        if field.kind == "select":
            assert field.default in field.options, field.name
        if field.kind == "number":
            assert field.min_value <= field.default <= field.max_value, field.name


def test_examples_match_the_form_and_pass_validation():
    for name, example in EXAMPLES.items():
        features = example["features"]
        assert set(features) == set(FIELDS_BY_NAME), name
        assert validate_features(features) == [], name


def test_widget_round_trip_maps_missing_to_null():
    state = FIELDS_BY_NAME["state"]
    broker = FIELDS_BY_NAME["brokered_by"]
    bed = FIELDS_BY_NAME["bed"]
    acres = FIELDS_BY_NAME["acre_lot"]
    assert to_widget_value(state, None) == UNKNOWN
    assert from_widget_value(state, UNKNOWN) is None
    assert from_widget_value(broker, "   ") is None
    assert from_widget_value(broker, " 101 ") == "101"
    assert from_widget_value(bed, 3.0) == 3
    assert from_widget_value(acres, 0.25) == 0.25


def test_validation_reports_bad_values_and_ranges():
    features = dict(next(iter(EXAMPLES.values()))["features"])
    features["zip_code"] = "abc"
    features["bed"] = 99  # out of range
    features["status"] = None  # required select
    fields = {name for name, _ in validate_features(features)}
    assert fields == {"zip_code", "bed", "status"}
