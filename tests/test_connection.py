"""Test for the Connection object."""

import json
import pytest

from tinysg import Connection


def test_connection_init_file_not_found_error():
    with pytest.raises(FileNotFoundError):
        Connection("/path/to/nothing")


def test_connection_init(fs, test_data):
    tmp = fs.create_file("json", contents=json.dumps(test_data))

    Connection(tmp.path)
