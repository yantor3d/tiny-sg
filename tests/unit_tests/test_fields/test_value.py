"""Test suite for the field create methods."""

import datetime
import json
import mock
import pytest

import tinysg.fields
import tinysg.utils

from tinysg.fields import FieldType

NOW = datetime.datetime(2000, 9, 1, 7, 30, 1)
TODAY = datetime.date(2000, 9, 1)


@pytest.mark.parametrize(
    "value,expected,field_spec",
    [
        (0, False, {"type": FieldType.BOOL.value}),
        (True, True, {"type": FieldType.BOOL.value}),
        (None, True, {"type": FieldType.BOOL.value, "default": True}),
        (None, None, {"type": FieldType.ENTITY.value, "link": "Shot"}),
        (
            {"type": "Shot", "id": 1, "name": "hello, world"},
            {"type": "Shot", "id": 1},
            {"type": FieldType.ENTITY.value, "link": "Shot"},
        ),
        ("a", "a", {"type": FieldType.ENUM.value, "default": "a", "values": ["a", "b", "c"]}),
        (None, "a", {"type": FieldType.ENUM.value, "default": "a", "values": ["a", "b", "c"]}),
        (1.0, 1.0, {"type": FieldType.FLOAT.value}),
        (None, 1.0, {"type": FieldType.FLOAT.value, "default": 1.0}),
        (None, None, {"type": FieldType.JSON.value}),
        ({"foo": "bar"}, {"foo": "bar"}, {"type": FieldType.JSON.value}),
        (None, None, {"type": FieldType.MULTI_ENTITY.value, "link": "Shot"}),
        (
            [{"type": "Shot", "id": 1, "name": "hello, world"}],
            [{"type": "Shot", "id": 1}],
            {"type": FieldType.MULTI_ENTITY.value, "link": "Shot"},
        ),
        (42, 42, {"type": FieldType.NUMBER.value}),
        (None, 13, {"type": FieldType.NUMBER.value, "default": 13}),
        ("", None, {"type": FieldType.TEXT.value}),
        ("Hello, world", "Hello, world", {"type": FieldType.TEXT.value}),
        ([], None, {"type": FieldType.TEXT_LIST.value}),
        (["A", "B", "C"], ["A", "B", "C"], {"type": FieldType.TEXT_LIST.value}),
    ],
    ids=[
        "bool (int)",
        "bool",
        "bool (default)",
        "entity",
        "entity (None)",
        "enum",
        "enum (default)",
        "float",
        "float (default)",
        "json (None)",
        "json",
        "multi_entity",
        "multi_entity (None)",
        "number",
        "number (default)",
        "text (empty)",
        "text",
        "text_list (empty)",
        "text_list",
    ],
)
def test_valid_field_value(value, expected, field_spec):
    field_spec["entity_type"] = "Asset"
    field_spec["name"] = "test_field"

    out_value = tinysg.fields.handle_value(value, field_spec)

    assert out_value == expected


@pytest.mark.parametrize(
    "value,expected,field_spec",
    [
        (TODAY, "2000-09-01", {"type": FieldType.DATE.value, "default": False}),
        (None, "2000-09-02", {"type": FieldType.DATE.value, "default": True}),
        (None, None, {"type": FieldType.DATE.value}),
    ],
    ids=[
        "date",
        "date (default)",
        "date (no default)",
    ],
)
def test_valid_date_field_value(value, expected, field_spec, mock_today):
    _check_field_value(value, expected, field_spec)


@pytest.mark.parametrize(
    "value,expected,field_spec",
    [
        (NOW, "2000-09-01 07:30:01", {"type": FieldType.DATE_TIME.value, "default": True}),
        (None, "2000-09-02 12:24:36", {"type": FieldType.DATE_TIME.value, "default": True}),
        (None, None, {"type": FieldType.DATE_TIME.value}),
    ],
    ids=[
        "datetime",
        "datetime (default)",
        "datetime (no default)",
    ],
)
def test_valid_datetime_field_value(value, expected, field_spec, mock_now):
    _check_field_value(value, expected, field_spec)


def _check_field_value(value, expected, field_spec):
    field_spec["entity_type"] = "Asset"
    field_spec["name"] = "test_field"

    out_value = tinysg.fields.handle_value(value, field_spec)

    assert out_value == expected


@pytest.mark.parametrize(
    "value,field_spec",
    [
        ("yes", {"type": FieldType.BOOL.value}),
        ("today", {"type": FieldType.DATE.value}),
        ("now", {"type": FieldType.DATE_TIME.value}),
        (1, {"type": FieldType.ENTITY.value, "link": "Shot"}),
        ({}, {"type": FieldType.ENTITY.value, "link": "Shot"}),
        ({"id": 1}, {"type": FieldType.ENTITY.value, "link": "Shot"}),
        ({"type": "Asset"}, {"type": FieldType.ENTITY.value, "link": "Shot"}),
        ({"type": "Asset", "id": 1}, {"type": FieldType.ENTITY.value, "link": "Shot"}),
        (3, {"type": FieldType.ENUM.value, "values": ["a", "b", "c"]}),
        ("x", {"type": FieldType.ENUM.value, "values": ["a", "b", "c"]}),
        ("red", {"type": FieldType.FLOAT.value}),
        (tinysg.utils.now(), {"type": FieldType.JSON.value}),
        ([1], {"type": FieldType.MULTI_ENTITY.value, "link": "Shot"}),
        ([{}], {"type": FieldType.MULTI_ENTITY.value, "link": "Shot"}),
        ([{"type": "Asset", "id": 1}], {"type": FieldType.MULTI_ENTITY.value, "link": "Shot"}),
        ("a", {"type": FieldType.NUMBER.value}),
        (123, {"type": FieldType.TEXT.value}),
        ([123], {"type": FieldType.TEXT_LIST.value}),
    ],
    ids=[
        "bool: invalid value",
        "date: invalid value",
        "datetime: invalid value",
        "entity: invalid valid",
        "entity: incomplete entity (no data)",
        "entity: incomplete entity (no id)",
        "entity: incomplete entity (no type)",
        "entity: invalid entity type",
        "enum: invalid type",
        "enum: invalid value",
        "float: invalid value",
        "json: invalid value",
        "multi_entity: invalid values",
        "multi_entity: incomplete values",
        "multi_entity: invalid entity type",
        "number: invalid value",
        "text: invalid value",
        "text list: invalid value",
    ],
)
def test_invalid_field_value(value, field_spec):
    field_spec["entity_type"] = "Asset"
    field_spec["name"] = "test_field"

    with pytest.raises(ValueError):
        tinysg.fields.handle_value(value, field_spec)
