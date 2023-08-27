"""Test suite for query filter spec."""

import json
import pytest

from tinysg import Connection
from tinysg.exceptions import FilterSpecError, SchemaError


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents=json.dumps(test_data))

    return Connection(tmp.path)


def test_resolve_link_invalid_filter_spec(connection):
    with pytest.raises(FilterSpecError):
        connection._resolve_filters(
            "Shot",
            [
                ["sequence.Sequence", {"type": "Sequence", "id": 1}],
            ],
        )


def test_resolve_link(connection):
    filters = connection._resolve_filters(
        "Shot",
        [
            ["sequence.Sequence.number", "is", "0100"],
        ],
    )

    assert filters == [
        ["sequence", "is", {"type": "Sequence", "id": 1}],
    ]


def test_resolve_deep_link(connection):
    filters = connection._resolve_filters(
        "Shot",
        [
            ["sequence.Sequence.project.Project.code", "is", "test"],
        ],
    )

    sequences = [
        {"type": "Sequence", "id": 1},
        {"type": "Sequence", "id": 2},
    ]

    assert filters == [
        ["sequence", "in", sequences],
    ]


def test_resolve_multiple_links(connection):
    filters = connection._resolve_filters(
        "Asset",
        [
            ["shots.Shot.sequence.Sequence.number", "is", "0100"],
            ["shots.Shot.number", "in", ["0010", "0020"]],
        ],
    )

    shots = [
        {"type": "Shot", "id": 1},
        {"type": "Shot", "id": 2},
    ]

    assert filters == [
        ["shots", "in", shots],
    ]
