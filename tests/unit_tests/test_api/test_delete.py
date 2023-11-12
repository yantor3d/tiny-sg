"""Test suite for the delete method."""

import json
import pytest

import tinysg.entity

from tinysg import Connection
from tinysg.exceptions import EntityNotFound, SchemaError


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return Connection(tmp.path)


def test_delete_invalid_entity_type_error(connection):
    with pytest.raises(
        SchemaError,
        match="A\(n\) 'InvalidEntityType' entity has not been registered.",
    ):
        connection.delete("InvalidEntityType", 1)


def test_delete_invalid_entity_id_error(connection):
    with pytest.raises(
        EntityNotFound,
        match="A\(n\) 'Asset' entity for id -1 does not exist",
    ):
        connection.delete("Asset", -1)


def test_delete(connection):
    result = connection.delete("Asset", 1)
    assert result is True, "Deleting an existing entity should return True."

    entity = connection.find_one("Asset", [["id", "is", 1]])
    assert entity is None, "Deleted entity should not be found in default query."

    result = connection.delete("Asset", 1)
    assert result is False, "Deleting an deleted entity should return False."

    entity = connection.find_one("Asset", [["id", "is", 1]], ["code", "shots"], retired_only=True)

    assert entity is not None, "Deleted entity should be found in an explicit query."
    assert entity["shots"] == [
        {"type": "Shot", "id": 1, "name": "0100.0010"},
        {"type": "Shot", "id": 3, "name": "0200.0010"},
    ]

    shots = connection.find_all("Shot", [], ["assets", "code"])

    handle = {
        "type": entity["type"],
        "id": entity["id"],
        "name": entity["code"],
    }

    for shot in shots:
        assert handle not in shot["assets"]
