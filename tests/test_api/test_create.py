"""Test suite for the create method."""

import json
import pytest
import re

from tinysg import Connection
from tinysg.exceptions import EntityNotFound, SchemaError


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return Connection(tmp.path)


def test_create_invalid_entity_type_error(connection):
    with pytest.raises(SchemaError):
        connection.create("InvalidEntityType", {})


def test_create_invalid_field_error(connection):
    with pytest.raises(SchemaError):
        connection.create("Asset", {"InvalidField": None})


def test_create_invalidate_field_value_error(connection):
    with pytest.raises(ValueError):
        connection.create(
            "Asset",
            {
                "code": "The Hero of our Story",
                "asset_type": "Villain",
            },
        )


def test_create_invalidate_entity_field_value_error(connection):
    with pytest.raises(EntityNotFound):
        connection.create(
            "Asset",
            {
                "code": "The Hero of our Story",
                "asset_type": "Character",
                "project": {"code": "test", "id": 99, "type": "Project"},
            },
        )

    with pytest.raises(EntityNotFound):
        connection.create(
            "Sequence",
            {
                "shots": [
                    {"type": "Shot", "id": 42},
                ],
            },
        )


def test_create_missing_required_field_error(connection):
    with pytest.raises(
        ValueError,
        match="Must set required fields for 'Asset' entity: asset_type, project",
    ):
        connection.create(
            "Asset",
            {
                "code": "the_hero",
                "name": "The Hero",
            },
        )


def test_create_non_unique_entity_error(connection):
    data = {
        "asset_type": "Character",
        "name": "The Hero",
        "code": "the_hero",
        "project": {"code": "test", "id": 1, "type": "Project"},
    }

    connection.create("Asset", data)

    with pytest.raises(
        ValueError,
        match="Cannot create 'Asset' entity because its identifier field values are not unique: asset_type, code, name, project",
    ):
        connection.create("Asset", data)


def test_create_non_unique_entity(connection):
    data = {
        "subject": "Hello, world!",
        "body": "The quick brown fox jumped over the lazy dog.",
    }

    a = connection.create("Note", data)
    b = connection.create("Note", data)

    assert a is not None
    assert b is not None
    assert a["id"] != b["id"]


def test_create(connection):
    result = connection.create(
        "Asset",
        {
            "asset_type": "Character",
            "name": "The Hero",
            "code": "the_hero",
            "project": {"code": "test", "id": 1, "type": "Project"},
        },
        [
            "asset_type",
            "code",
            "project",
        ],
    )

    assert result == {
        "asset_type": "Character",
        "code": "the_hero",
        "id": 4,
        "project": {"name": "test", "id": 1, "type": "Project"},
        "type": "Asset",
    }

    result = connection.find_one(
        "Asset",
        [
            ["id", "is", 4],
        ],
    )

    assert result is not None
