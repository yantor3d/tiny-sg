"""Test suite config."""

import datetime
import json
import mock
import os
import pytest

import tinysg.utils


@pytest.fixture(scope="session")
def test_data():
    db_path = os.path.join(os.path.dirname(__file__), "db.json")

    with open(db_path, "r") as fp:
        return json.load(fp)


@pytest.fixture(scope="function")
def mock_today():
    # Force coverage of the function.
    tinysg.utils.today()

    today = datetime.date(2000, 9, 2)

    mock_date = mock.MagicMock()
    mock_date.return_value = today

    with mock.patch("tinysg.utils.today", new=mock_date):
        yield mock_date


@pytest.fixture(scope="function")
def mock_now():
    # Force coverage of the function.
    tinysg.utils.now()

    now = datetime.datetime(2000, 9, 2, 12, 24, 36)

    mock_datetime = mock.MagicMock()
    mock_datetime.return_value = now

    with mock.patch("tinysg.utils.now", new=mock_datetime):
        yield mock_datetime
