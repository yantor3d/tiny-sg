"""Test suite for the schema editing methods."""

import json
import pytest

from tinysg import Connection
from tinysg.exceptions import SchemaError


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return Connection(tmp.path)


def test_schema_entity_create(connection):
    assert not connection.schema_entity_check("NewEntity")

    result = connection.schema_entity_create("NewEntity")

    assert result is not None
    assert result["entity_type"] == "NewEntity"
    assert connection.schema_entity_read("NewEntity") == result
    assert connection.schema_entity_check("NewEntity")

    assert "NewEntity" in connection.schema_entity_read_all()


def test_schema_entity_create_error(connection):
    with pytest.raises(
        SchemaError,
        match="A\(n\) 'Asset' entity has already been registered.",
    ):
        connection.schema_entity_create("Asset")
