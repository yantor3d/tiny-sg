"""Test suite for the find_one/find_all methods."""

import json
import pytest

from tinysg import Connection
from tinysg.exceptions import FilterSpecError, SchemaError


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return Connection(tmp.path)


def test_find_invalid_entity_type(connection):
    with pytest.raises(
        SchemaError,
        match="A\(n\) 'InvalidEntityType' entity has not been registered.",
    ):
        connection.find_one("InvalidEntityType", [])

    with pytest.raises(
        SchemaError,
        match="A\(n\) 'InvalidEntityType' entity has not been registered.",
    ):
        connection.find_one("Asset", [["shots.InvalidEntityType.code", "is", 1]])


def test_find_one(connection):
    result = connection.find_one(
        "Asset",
        filters=[
            ["name", "is", "our_hero"],
        ],
        return_fields=[
            "asset_type",
        ],
    )

    assert result is not None
    assert set(result.keys()) == {"type", "id", "asset_type"}

    _check_entity(result, {"asset_type": "Character", "id": 1})


def test_find_one_by_id(connection):
    result = connection.find_one(
        "Asset",
        filters=[
            ["id", "is", 1],
        ],
        return_fields=[
            "asset_type",
        ],
    )

    assert result is not None
    assert set(result.keys()) == {"type", "id", "asset_type"}

    _check_entity(result, {"asset_type": "Character", "id": 1})


def test_find_one_no_result(connection):
    result = connection.find_one(
        "Asset",
        [
            ["name", "is", "sir_not_appearing_in_this_film"],
        ],
    )

    assert result is None


def test_find_all_no_filters(connection):
    results = connection.find_all("Asset", [])

    assert len(results) == 3


def test_find_all_invalid_field(connection):
    with pytest.raises(
        SchemaError,
        match="Entity 'Asset' has no 'foobar' field.",
    ):
        connection.find_all("Asset", [["foobar", "is", 1]])

    with pytest.raises(
        SchemaError,
        match="Entity 'Shot' has no 'foobar' field.",
    ):
        connection.find_all("Asset", [["shots.Shot.foobar", "is", 1]])

    with pytest.raises(
        FilterSpecError,
        match="Cannot do deep filter on non-link field 'Asset.asset_type'.",
    ):
        connection.find_all("Asset", [["asset_type.Shot.foobar", "is", 1]])


def test_find_all(connection):
    results = connection.find_all(
        "Asset",
        [
            ["asset_type", "is", "Character"],
        ],
    )

    assert len(results) == 2

    project = {"type": "Project", "id": 1, "name": "test"}

    _check_entity(results[0], {"asset_type": "Character", "id": 1, "project": project})
    _check_entity(results[1], {"asset_type": "Character", "id": 2, "project": project})


def test_find_all_link_entity_field(connection):
    results = connection.find_all(
        "Shot",
        [
            ["sequence.Sequence.number", "is", "0100"],
        ],
        [
            "number",
            "project",
            "sequence",
            "sequence.Sequence.number",
        ],
    )

    assert len(results) == 2

    project = {"type": "Project", "id": 1, "name": "test"}
    seq = {"type": "Sequence", "id": 1, "name": "0100 - A", "number": "0100"}

    _check_entity(results[0], {"number": "0010", "id": 1, "project": project, "sequence": seq})
    _check_entity(results[1], {"number": "0020", "id": 2, "project": project, "sequence": seq})


def test_find_all_link_entity_field_no_results(connection):
    results = connection.find_all(
        "Shot",
        [
            ["sequence.Sequence.number", "is", "9999"],
        ],
    )

    assert not results


def test_find_all_no_results(connection):
    results = connection.find_all(
        "Asset",
        [
            ["asset_type", "is", "Nothing"],
        ],
    )

    assert results == []


def test_find_all_link_multi_entity_field(connection):
    results = connection.find_all(
        "Asset",
        [
            ["shots.Shot.sequence.Sequence.number", "is", "0100"],
            ["shots.Shot.number", "in", ["0010", "0020"]],
        ],
        [
            "code",
            "asset_type",
            "name",
            "shots",
            "shots.Shot.sequence",
            "shots.Shot.sequence.Sequence.number",
            "shots.Shot.number",
        ],
    )

    assert len(results) == 3

    for result in results:
        shots = result.get("shots")
        assert shots is not None

        for shot in shots:
            seq = shot.get("sequence")
            assert seq is not None

            assert "number" in seq


def test_find_all_link_multi_entity_field_no_results(connection):
    results = connection.find_all(
        "Asset",
        [
            ["shots.Shot.sequence.Sequence.number", "is", "0100"],
            ["shots.Shot.number", "is", "9999"],
        ],
    )

    assert not results


def _check_entity(actual, expected):
    for field, expected_value in expected.items():
        assert field in actual, f"Field {field} not in result."
        assert actual[field] == expected_value, f"Field '{field}' does not have the expected value"


def test_find_by_entity(connection):
    results = connection.find_all(
        "Shot",
        [
            ["sequence", "is", {"type": "Sequence", "id": 2}],
        ],
        [
            "code",
        ],
    )

    assert len(results) == 1
    assert results[0]["code"] == "0200.0010"


def test_find_by_not_entity(connection):
    results = connection.find_all(
        "Shot",
        [
            ["sequence", "is_not", {"type": "Sequence", "id": 1}],
        ],
        [
            "code",
        ],
    )

    assert len(results) == 1
    assert results[0]["code"] == "0200.0010"


def test_find_by_multi_entity(connection):
    results = connection.find_all(
        "Sequence",
        [
            ["shots", "is", {"type": "Shot", "id": 3}],
        ],
        [
            "number",
        ],
    )

    assert len(results) == 1
    assert results[0]["number"] == "0200"


def test_find_by_not_multi_entity(connection):
    results = connection.find_all(
        "Sequence",
        [
            ["shots", "is_not", {"type": "Shot", "id": 3}],
        ],
        [
            "number",
        ],
    )

    assert len(results) == 1
    assert results[0]["number"] == "0100"
