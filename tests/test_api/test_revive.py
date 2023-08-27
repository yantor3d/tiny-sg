"""Test suite for the revive method."""

import json
import pytest

import tinysg.entity

from tinysg import Connection
from tinysg.exceptions import EntityNotFound, SchemaError


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return Connection(tmp.path)


def test_revive_invalid_entity_type_error(connection):
    with pytest.raises(SchemaError):
        connection.revive("InvalidEntityType", 1)


def test_revive_invalid_entity_id_error(connection):
    with pytest.raises(EntityNotFound):
        connection.revive("Asset", -1)


def test_revive(connection):
    result = connection.delete("Asset", 1)
    assert result is True, "Deleting an entity should return True."

    result = connection.revive("Asset", 1)
    assert result is True, "Reviving an retired entity should return True."

    result = connection.revive("Asset", 1)
    assert result is False, "Reviving an existing entity should return False."

    asset = connection.find_one("Asset", [["id", "is", 1]], ["code", "shots"])

    assert asset is not None
    assert asset["shots"] == [
        {"type": "Shot", "id": 1, "name": "0100.0010"},
        {"type": "Shot", "id": 3, "name": "0200.0010"},
    ]

    shots = connection.find_all("Shot", [["id", "in", [1, 3]]], ["assets"])

    handle = {
        "type": asset["type"],
        "id": asset["id"],
        "name": asset["code"],
    }

    for shot in shots:
        assert handle in shot["assets"]


def test_revive_with_an_obsolete_link_1(connection):
    connection.delete("Shot", 1)
    connection.delete("Sequence", 1)

    result = connection.revive("Shot", 1)
    assert result is True, "Reviving an retired entity should return True."

    shot = connection.find_one("Shot", [["id", "is", 1]], ["sequence"])

    assert shot is not None

    with pytest.raises(KeyError):
        shot["sequence"]


def test_revive_with_an_obsolete_link_2(connection):
    connection.delete("Sequence", 1)
    connection.delete("Shot", 1)

    result = connection.revive("Shot", 1)
    assert result is True, "Reviving an retired entity should return True."

    shot = connection.find_one("Shot", [["id", "is", 1]], ["sequence"])

    assert shot is not None

    with pytest.raises(KeyError):
        shot["sequence"]


def test_revive_with_some_obsolete_links(connection):
    connection.delete("Asset", 1)
    connection.delete("Shot", 1)

    result = connection.revive("Asset", 1)
    assert result is True, "Reviving an retired entity should return True."

    asset = connection.find_one("Asset", [["id", "is", 1]], ["code", "shots"])

    assert asset is not None
    assert asset["shots"] == [
        {"type": "Shot", "id": 3, "name": "0200.0010"},
    ]


def test_revive_with_all_obsolete_links(connection):
    connection.delete("Asset", 1)

    connection.delete("Shot", 1)
    connection.delete("Shot", 2)
    connection.delete("Shot", 3)

    result = connection.revive("Asset", 1)
    assert result is True, "Reviving an retired entity should return True."

    asset = connection.find_one("Asset", [["id", "is", 1]], ["code", "shots"])

    assert asset is not None

    with pytest.raises(KeyError):
        asset["shots"]
