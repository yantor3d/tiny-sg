"""Test suite for the field crud methods."""

import json
import pytest

from tinysg import Connection
from tinysg.fields import FieldType
from tinysg.exceptions import SchemaError


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return Connection(tmp.path)


@pytest.fixture(scope="function")
def new_connection(fs):
    tmp = fs.create_file("db", contents="{}")

    return Connection(tmp.path)


def test_schema_field_create_error(connection):
    with pytest.raises(SchemaError):
        connection.schema_field_create("Asset", "name", {})


@pytest.mark.parametrize(
    "field_name,properties",
    [
        ("checkbox", {"type": FieldType.BOOL.value, "default": True}),
        ("due_date", {"type": FieldType.DATE.value, "default": False}),
        ("created_at", {"type": FieldType.DATE_TIME.value, "default": True}),
        # TODO: Calculate table for entity field links
        (
            "project",
            {
                "type": FieldType.ENTITY.value,
                "link": ["Project"],
                "table": "Connection:Note.project",
            },
        ),
        (
            "entity",
            {
                "type": FieldType.ENTITY.value,
                "link": ["Asset", "Shot"],
                "table": "Connection:Note.entity",
            },
        ),
        (
            "status",
            {
                "type": FieldType.ENUM.value,
                "default": "n/a",
                "values": ["n/a", "wip", "done"],
            },
        ),
        ("cost", {"type": FieldType.FLOAT.value}),
        ("metadata", {"type": FieldType.JSON.value}),
        # TODO: Calculate table for multi-entity field links
        (
            "assignees",
            {
                "type": FieldType.MULTI_ENTITY.value,
                "link": ["User"],
                "table": "Connection:Note.assignees",
            },
        ),
        (
            "replies",
            {
                "type": FieldType.MULTI_ENTITY.value,
                "link": ["Note"],
                "table": "Connection:Note.replies",
            },
        ),
        ("story_points", {"type": FieldType.NUMBER.value}),
        ("description", {"type": FieldType.TEXT.value}),
    ],
    ids=[
        "bool",
        "date",
        "datetime",
        "entity self",
        "entity other",
        "enum",
        "float",
        "json",
        "multi_entity other",
        "multi_entity self",
        "number",
        "text",
    ],
)
def test_add_field(connection, field_name, properties):
    entity_type = "Note"

    connection.schema_entity_create(entity_type)
    assert not connection.schema_field_check(entity_type, field_name)

    connection.schema_field_create(entity_type, field_name, properties)
    result = connection.schema_field_read(entity_type, field_name)

    assert result is not None
    assert result["entity_type"] == entity_type
    assert result["name"] == field_name

    for key, value in properties.items():
        assert result[key] == value, f"Field property {key} not set."


def test_delete_field(new_connection):
    entity_type = "Task"
    field_name = "name"

    connection = new_connection
    connection.schema_entity_create(entity_type)
    connection.schema_field_create(entity_type, field_name, {"type": "text"})

    connection._db.table(entity_type).insert_multiple(
        [
            {"name": "Fizz", "value": 3},
            {"name": "Buzz", "value": 5},
            {"name": "FizzBuzz", "value": 15},
        ]
    )

    results = connection._db.table(entity_type).all()
    assert all([field_name in result for result in results])

    connection.schema_field_delete(entity_type, field_name)
    assert not connection.schema_field_check(entity_type, field_name)

    results = connection._db.table(entity_type).all()
    assert all([field_name not in result for result in results])


def test_update_field(new_connection):
    entity_type = "Task"
    field_name = "status"

    status_list = ["n/a", "wip", "done"]
    connection = new_connection
    connection.schema_entity_create(entity_type)
    connection.schema_field_create(
        entity_type,
        field_name,
        {
            "type": "enum",
            "default": "n/a",
            "values": status_list,
        },
    )

    field = connection.schema_field_read(entity_type, field_name)
    assert field["default"] == "n/a"
    assert field["values"] == status_list

    field = connection.schema_field_update(entity_type, field_name, {"default": "wip"})
    assert field["default"] == "wip"
    assert field["values"] == status_list


def test_update_field_error(new_connection):
    entity_type = "Task"
    field_name = "due_date"

    connection = new_connection
    connection.schema_entity_create(entity_type)
    connection.schema_field_create(entity_type, field_name, {"type": "date"})

    with pytest.raises(ValueError):
        connection.schema_field_update(entity_type, field_name, {"default": "this_week"})
