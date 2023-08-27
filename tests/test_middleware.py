"""Test for the pivot table middleware."""

import json
import pytest

from tinydb import TinyDB, JSONStorage
from tinysg.middleware import PivotTableMiddleware


@pytest.fixture
def db(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return TinyDB(tmp.path, storage=PivotTableMiddleware(JSONStorage))


def test_middleware_read(db):
    data = db.storage.read()

    schema = data.get("_schema", {})

    for entity_info in schema.values():
        entity_type = entity_info["entity_type"]

        table = data.get(entity_type, {})

        fields = ["id", "type"]

        for field in entity_info["fields"]:
            if "table" in field:
                fields.append(field["name"])

        errant_fields = set()

        for __, entity in table.items():
            for field in fields:
                if field not in entity:
                    errant_fields.add(field)

        errant_fields_str = ",".join(sorted(errant_fields))

        assert not errant_fields, f"{entity_type} is missing {errant_fields_str}"


def test_middleware_write(db):
    data = db.storage.read()
    data = db.storage.write(data)

    schema = data.get("_schema", {})

    for entity_info in schema.values():
        entity_type = entity_info["entity_type"]

        table = data.get(entity_type, {})

        fields = ["id", "type"]

        for field in entity_info["fields"]:
            if "table" in field:
                fields.append(field["name"])

        errant_fields = set()

        for __, entity in table.items():
            for field in fields:
                if field in entity:
                    errant_fields.add(field)

        errant_fields_str = ",".join(sorted(errant_fields))

        assert not errant_fields, f"{entity_type} still has {errant_fields_str}"
