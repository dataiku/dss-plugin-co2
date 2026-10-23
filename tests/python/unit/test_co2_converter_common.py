import importlib.util
from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import Mock

import pandas as pd
import pytest


@pytest.fixture
def common(monkeypatch):
    """Load the helpers with an isolated Dataiku SDK stub for each test."""
    dataiku = ModuleType("dataiku")
    customrecipe = ModuleType("dataiku.customrecipe")
    dataiku.Dataset = Mock()
    dataiku.customrecipe = customrecipe
    customrecipe.get_recipe_config = Mock(return_value={})
    customrecipe.get_input_names_for_role = Mock(return_value=["input"])
    customrecipe.get_output_names_for_role = Mock(return_value=["output"])
    monkeypatch.setitem(sys.modules, "dataiku", dataiku)
    monkeypatch.setitem(sys.modules, "dataiku.customrecipe", customrecipe)

    module_path = Path(__file__).resolve().parents[3] / "python-lib" / "co2_converter_common.py"
    spec = importlib.util.spec_from_file_location("co2_converter_common", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("point, expected", [
    ("POINT(2.352222 48.856614)", [2.352222, 48.856614]),
    ("POINT(-180 -90)", [-180.0, -90.0]),
    ("POINT(180 90)", [180.0, 90.0]),
    ("POINT( 2.5 48.5 )", [2.5, 48.5]),
    ("POINT(181 45)", [None, None]),
    ("POINT(-181 45)", [None, None]),
    ("POINT(2 91)", [None, None]),
    ("POINT(2 -91)", [None, None]),
    ("POINT(foo bar)", [None, None]),
    ("invalid", [None, None]),
    ("", [None, None]),
])
def test_parse_wkt_point(common, point, expected):
    assert common.parse_wkt_point(point) == expected


def test_date_chunk_single_day_includes_next_day(common):
    assert common.date_chunk("2024-02-29", "2024-02-29", 10) == [
        ["2024-02-29", "2024-03-01"]
    ]


@pytest.mark.parametrize("end", ["2024-01-04", "2024-01-20", "2024-02-05"])
def test_date_chunk_covers_range_in_order_with_bounded_chunks(common, end):
    chunks = common.date_chunk("2024-01-01", end, 10)
    expected = pd.date_range("2024-01-01", end).strftime("%Y-%m-%d").tolist()

    assert [day for chunk in chunks for day in chunk] == expected
    assert all(1 <= len(chunk) <= 10 for chunk in chunks)


@pytest.mark.parametrize("start, end", [
    ("invalid", "2024-01-02"),
    ("2024-01-01", "2024-02-30"),
])
def test_date_chunk_rejects_invalid_dates(common, start, end):
    with pytest.raises(ValueError):
        common.date_chunk(start, end, 10)


def test_merge_uses_latest_preceding_time_and_preserves_unmatched_rows(common):
    left = pd.DataFrame({"time": [5, 1, 3], "consumption": [50, 10, 30]})
    right = pd.DataFrame({"co2_time": [4, 2], "intensity": [400, 200]})

    result = common.merge_w_nearest_keys(left, right, "time", "co2_time")

    expected = pd.DataFrame({
        "time": [1, 3, 5],
        "consumption": [10, 30, 50],
        "co2_time": [float("nan"), 2.0, 4.0],
        "intensity": [float("nan"), 200.0, 400.0],
    })
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_merge_matches_exact_times_within_location(common):
    left = pd.DataFrame({
        "time": [4, 3, 2], "latitude": [48, 49, 48], "longitude": [2, 3, 2]
    })
    right = pd.DataFrame({
        "co2_time": [3, 2], "latitude": [49, 48], "longitude": [3, 2],
        "intensity": [300, 200],
    })

    result = common.merge_w_nearest_keys(
        left, right, "time", "co2_time", by=["latitude", "longitude"]
    )

    assert result["time"].tolist() == [2, 3, 4]
    assert result["intensity"].tolist() == [200, 300, 200]


def test_get_geopoint_column_names_without_collision(common):
    assert common.get_geopoint_column_names(["date", "consumption"]) == (
        "extracted_geopoint", "extracted_geopoint_longitude", "extracted_geopoint_latitude"
    )


@pytest.mark.xfail(strict=True, reason="The column collision condition is always truthy")
@pytest.mark.parametrize("existing", [
    "extracted_geopoint", "extracted_geopoint_longitude", "extracted_geopoint_latitude"
])
def test_get_geopoint_column_names_avoids_collision(common, existing):
    assert common.get_geopoint_column_names([existing]) == (
        "extracted_geopoint_42", "extracted_geopoint_longitude_42", "extracted_geopoint_latitude_42"
    )


def test_get_input_output_reads_input_and_returns_output_dataset(common):
    frame = pd.DataFrame({"date": ["2024-01-01"], "consumption": [10]})
    input_dataset = Mock()
    input_dataset.get_dataframe.return_value = frame
    output_dataset = Mock()
    common.dataiku.Dataset.side_effect = {"input": input_dataset, "output": output_dataset}.__getitem__

    result_frame, result_output, columns = common.get_input_output()

    assert result_frame is frame
    assert result_output is output_dataset
    assert columns.tolist() == ["date", "consumption"]
    common.get_input_names_for_role.assert_called_once_with("input_ds")
    common.get_output_names_for_role.assert_called_once_with("output_ds")
    input_dataset.get_dataframe.assert_called_once_with()


def test_get_coordinates_returns_selected_column(common):
    common.get_recipe_config.return_value = {"coordinates": "location"}
    frame = pd.DataFrame({"location": ["POINT(2 48)", "POINT(3 49)"]})

    assert common.get_coordinates(frame) == "location"


@pytest.mark.parametrize("config, values, message", [
    ({}, ["POINT(2 48)"], "No coordinate column was selected"),
    ({"coordinates": "missing"}, ["POINT(2 48)"], "Not able to find the 'missing' column"),
    ({"coordinates": "location"}, ["POINT(2 48)", None], "Empty coordinates found"),
    ({"coordinates": "location"}, ["POINT(2 48)", "invalid"], "Invalid coordinates"),
])
def test_get_coordinates_rejects_invalid_input(common, config, values, message):
    common.get_recipe_config.return_value = config

    with pytest.raises(ValueError, match=message):
        common.get_coordinates(pd.DataFrame({"location": values}))


def test_get_api_token_returns_configured_token(common):
    common.get_recipe_config.return_value = {"api_configuration_preset": {"APITOKEN": "test-token"}}

    assert common.get_api_token() == "test-token"


@pytest.mark.parametrize("preset", [{}, {"APITOKEN": ""}, {"APITOKEN": None}])
def test_get_api_token_rejects_empty_token(common, preset):
    common.get_recipe_config.return_value = {"api_configuration_preset": preset}

    with pytest.raises(ValueError, match="No electricityMap API token found"):
        common.get_api_token()


@pytest.mark.parametrize("function, key", [
    ("get_date_column_name", "date_column_name"),
    ("get_consumption_column_name", "consumption_column_name"),
])
def test_selected_column_returns_existing_column(common, function, key):
    common.get_recipe_config.return_value = {key: "selected"}

    assert getattr(common, function)(pd.DataFrame({"selected": [1]})) == "selected"


@pytest.mark.parametrize("function, key, message", [
    ("get_date_column_name", "date_column_name", "No date column was selected"),
    ("get_consumption_column_name", "consumption_column_name", "No consumption column was selected"),
])
@pytest.mark.parametrize("config_value", [None, "", "missing"])
def test_selected_column_rejects_missing_selection_or_column(common, function, key, message, config_value):
    common.get_recipe_config.return_value = {key: config_value}
    if config_value == "missing":
        message = "Not able to find the 'missing' column"

    with pytest.raises(ValueError, match=message):
        getattr(common, function)(pd.DataFrame({"existing": [1]}))
