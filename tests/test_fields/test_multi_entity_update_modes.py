"""Test suite for the field update methods."""

import datetime
import json
import mock
import pytest

import tinysg.fields
import tinysg.utils

from tinysg import Connection
from tinysg.fields import FieldType, UpdateMode
from tinysg.exceptions import SchemaError

ASSETS = [
    {"type": "Asset", "id": 1},
    {"type": "Asset", "id": 2},
    {"type": "Asset", "id": 3},
    {"type": "Asset", "id": 4},
]


def test_update_multi_entity_field_update_mode_error():
    with pytest.raises(ValueError):
        tinysg.fields.update_multi_entity_field([], [], "foo")


@pytest.mark.parametrize(
    "old,new,expected",
    (
        (
            [],
            [],
            [],
        ),
        (
            [],
            [ASSETS[0]],
            [ASSETS[0]],
        ),
        (
            [ASSETS[0]],
            [],
            [ASSETS[0]],
        ),
        (
            [ASSETS[0], ASSETS[1]],
            [ASSETS[0]],
            [ASSETS[0], ASSETS[1]],
        ),
        (
            [ASSETS[0], ASSETS[1]],
            [ASSETS[0], ASSETS[2]],
            [ASSETS[0], ASSETS[1], ASSETS[2]],
        ),
        (
            [ASSETS[0], ASSETS[1]],
            [ASSETS[2], ASSETS[3]],
            ASSETS[:],
        ),
    ),
    ids=[
        "no values",
        "no old values",
        "no new values",
        "old and new values - new values exist",
        "old and new values - new values mixed",
        "old and new values - new values unique",
    ],
)
def test_update_mode_add(old, new, expected):
    actual = tinysg.fields.update_multi_entity_field(old, new, UpdateMode.ADD.value)

    assert expected == actual


@pytest.mark.parametrize(
    "old,new,expected",
    (
        (
            [],
            [],
            [],
        ),
        (
            [],
            [ASSETS[0]],
            [],
        ),
        (
            [ASSETS[0]],
            [],
            [ASSETS[0]],
        ),
        (
            [ASSETS[0], ASSETS[1]],
            [ASSETS[0]],
            [ASSETS[1]],
        ),
        (
            [ASSETS[0], ASSETS[1]],
            [ASSETS[0], ASSETS[2]],
            [ASSETS[1]],
        ),
        (
            [ASSETS[0], ASSETS[1]],
            [ASSETS[2], ASSETS[3]],
            [ASSETS[0], ASSETS[1]],
        ),
    ),
    ids=[
        "no values",
        "no old values",
        "no new values",
        "old and new values - new values exist",
        "old and new values - new values mixed",
        "old and new values - new values unique",
    ],
)
def test_update_mode_remove(old, new, expected):
    actual = tinysg.fields.update_multi_entity_field(old, new, UpdateMode.REMOVE.value)
    actual = [each for each in actual if "remove" not in each]

    assert expected == actual


@pytest.mark.parametrize(
    "old,new,expected",
    (
        (
            [],
            [],
            [],
        ),
        (
            [],
            [ASSETS[0]],
            [ASSETS[0]],
        ),
        (
            [ASSETS[0]],
            [],
            [],
        ),
        (
            [ASSETS[0], ASSETS[1]],
            [ASSETS[1], ASSETS[2]],
            [ASSETS[1], ASSETS[2]],
        ),
    ),
    ids=["no values", "no old values", "no new values", "new values"],
)
def test_update_mode_set(old, new, expected):
    actual = tinysg.fields.update_multi_entity_field(old, new, UpdateMode.SET.value)
    actual = [each for each in actual if "remove" not in each]

    assert expected == actual
