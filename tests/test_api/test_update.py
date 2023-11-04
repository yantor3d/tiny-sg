"""Test suite for the update methods."""

import json
import pytest

from tinysg import Connection
from tinysg.exceptions import EntityNotFound, SchemaError


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return Connection(tmp.path)


def test_update_invalid_entity_type_error(connection):
    with pytest.raises(SchemaError):
        connection.update("InvalidEntityType", -1, {})


def test_update_invalid_entity_id_error(connection):
    with pytest.raises(EntityNotFound):
        connection.update("Asset", -1, {})


def test_update_invalid_field_error(connection):
    with pytest.raises(SchemaError):
        connection.update("Asset", 1, {"InvalidField": None})


def test_update_invalidate_field_value_error(connection):
    data = {
        "asset_type": "Villain",
    }

    with pytest.raises(ValueError):
        connection.update("Asset", 1, data)


@pytest.mark.parametrize(
    "data",
    (
        {"project": {"id": 99, "type": "Project"}},
        {"shots": [{"type": "Shot", "id": 42}]},
    ),
    ids=["entity field", "multi-entity field"],
)
def test_update_invalid_entity_field_value_error(connection, data):
    with pytest.raises(EntityNotFound):
        connection.update("Sequence", 1, data)


def test_update_invalid_multi_entity_update_mode_error(connection):
    with pytest.raises(ValueError):
        connection.update("Sequence", 1, {}, {"shots": "update"})


def test_update_invalid_field_multi_entity_update_mode_error(connection):
    with pytest.raises(ValueError):
        connection.update("Sequence", 1, {}, {"project": "add"})


def test_update(connection):
    result = connection.create(
        "Asset",
        {
            "asset_type": "Character",
            "code": "the_hero",
            "name": "The Hero",
            "project": {"code": "test", "id": 1, "type": "Project"},
            "status": "Active",
        },
    )

    result = connection.update(
        result["type"],
        result["id"],
        {
            "name": "The Villain",
            "code": "the_villain",
            "project": {"id": 2, "type": "Project"},
            "status": None,
        },
    )

    assert result is not None
    assert result["code"] == "the_villain"
    assert result["project"] == {"name": "prod", "id": 2, "type": "Project"}


def test_update_required_field_error(connection):
    result = connection.create(
        "Asset",
        {
            "asset_type": "Character",
            "code": "the_hero",
            "name": "The Hero",
            "project": {"code": "test", "id": 1, "type": "Project"},
        },
    )

    with pytest.raises(
        ValueError,
        match="Cannot unset required fields for 'Asset' entity: asset_type",
    ):
        connection.update(
            result["type"],
            result["id"],
            {
                "asset_type": None,
            },
        )


def test_update_multi_entity_field(connection):
    result = connection.create(
        "Asset",
        {
            "asset_type": "Character",
            "code": "the_hero",
            "name": "The Hero",
            "project": {"code": "test", "id": 1, "type": "Project"},
        },
    )

    entity_type = result["type"]
    entity_id = result["id"]

    _set_shots(connection, entity_type, entity_id)
    _add_shots(connection, entity_type, entity_id)
    _remove_shots(connection, entity_type, entity_id)
    _clear_shots(connection, entity_type, entity_id)


def _set_shots(connection, entity_type, entity_id):
    result = connection.update(
        entity_type,
        entity_id,
        {
            "shots": [{"type": "Shot", "id": 1}],
        },
        multi_entity_update_modes={
            "shots": "set",
        },
    )

    assert result is not None
    assert result["shots"] == [
        {"type": "Shot", "id": 1, "name": "0100.0010"},
    ]


def _add_shots(connection, entity_type, entity_id):
    result = connection.update(
        entity_type,
        entity_id,
        {
            "shots": [{"type": "Shot", "id": 2}],
        },
        multi_entity_update_modes={
            "shots": "add",
        },
    )

    assert result is not None
    assert result["shots"] == [
        {"type": "Shot", "id": 1, "name": "0100.0010"},
        {"type": "Shot", "id": 2, "name": "0100.0020"},
    ]


def _remove_shots(connection, entity_type, entity_id):
    result = connection.update(
        entity_type,
        entity_id,
        {
            "shots": [{"type": "Shot", "id": 1}],
        },
        multi_entity_update_modes={
            "shots": "remove",
        },
    )

    assert result is not None
    assert result["shots"] == [
        {"type": "Shot", "id": 2, "name": "0100.0020"},
    ]


def _clear_shots(connection, entity_type, entity_id):
    result = connection.update(
        entity_type,
        entity_id,
        {
            "shots": [],
        },
        multi_entity_update_modes={
            "shots": "set",
        },
    )

    assert result is not None
    assert "shots" not in result
