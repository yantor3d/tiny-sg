"""Test suite for the field spec validate functions."""

import json
import pytest

import tinysg.fields

from tinysg.fields import FieldType


@pytest.mark.parametrize(
    "properties",
    [
        {"type": FieldType.BOOL.value},
        {"type": FieldType.DATE.value, "default": False},
        {"type": FieldType.DATE_TIME.value, "default": True},
        {"type": FieldType.ENTITY.value, "link": "Shot"},
        {"type": FieldType.ENUM.value, "default": "a", "values": ["a", "b", "c"]},
        {"type": FieldType.FLOAT.value},
        {"type": FieldType.JSON.value},
        {"type": FieldType.MULTI_ENTITY.value, "link": "Shot"},
        {"type": FieldType.NUMBER.value},
        {"type": FieldType.TEXT.value},
        {"type": FieldType.TEXT_LIST.value},
    ],
    ids=[
        "bool",
        "date",
        "datetime",
        "entity",
        "enum",
        "float",
        "json",
        "multi_entity",
        "number",
        "text",
        "text_list",
    ],
)
def test_valid_field_spec(properties):
    tinysg.fields.validate_spec("field_name", properties)


@pytest.mark.parametrize(
    "properties",
    [
        {},
        {"type": "banana"},
        {"type": FieldType.BOOL.value, "default": "yes"},
        {"type": FieldType.DATE.value, "default": "today"},
        {"type": FieldType.DATE_TIME.value, "default": "now"},
        {"type": FieldType.ENTITY.value},
        {
            "type": FieldType.ENTITY.value,
            "link": "Shot",
            "default": {"type": "Shot", "id": 1},
        },
        {"type": FieldType.ENUM.value},
        {"type": FieldType.ENUM.value, "default": "x", "values": ["a", "b", "c"]},
        {"type": FieldType.FLOAT.value, "default": "x"},
        {"type": FieldType.JSON.value, "default": "x"},
        {"type": FieldType.MULTI_ENTITY.value},
        {
            "type": FieldType.MULTI_ENTITY.value,
            "link": "Shot",
            "default": {"type": "Shot", "id": 1},
        },
        {"type": FieldType.NUMBER.value, "default": "x"},
        {"type": FieldType.TEXT.value, "default": "x"},
        {"type": FieldType.TEXT_LIST.value, "default": "x"},
    ],
    ids=[
        "any: no properties",
        "invalid data type",
        "bool: invalid default",
        "date: invalid default",
        "datetime: invalid default",
        "entity: no link",
        "entity: has default",
        "enum: no values",
        "enum: invalid default",
        "float: invalid default",
        "json: has default",
        "multi_entity: no link",
        "multi_entity: has default",
        "number: invalid default",
        "text: has default",
        "text list: has default",
    ],
)
def test_invalid_field_spec(properties):
    with pytest.raises(ValueError):
        tinysg.fields.validate_spec("field_name", properties)
