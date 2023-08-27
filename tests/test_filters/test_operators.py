"""Test suite for query filter functions."""

import datetime
import pytest

import tinysg.fields
import tinysg.utils

from tinysg import Connection
from tinysg.exceptions import FilterSpecError

TODAY = datetime.date


@pytest.fixture(scope="function")
def connection(fs, test_data):
    tmp = fs.create_file("db", contents="{}")

    return Connection(tmp.path)


@pytest.mark.parametrize(
    "filter_spec",
    [
        ["field", "has", "value"],
        ["field", "is"],
    ],
    ids=[
        "Invalid operator",
        "Incomplete spec",
    ],
)
def test_get_invalid_filter(connection, filter_spec):
    with pytest.raises(FilterSpecError):
        connection._filter_to_query(filter_spec)


def test_parse_deep_field_invalid_spec_error():
    with pytest.raises(FilterSpecError):
        tinysg.fields.parse_deep_field("field.Entity")


def test_between(connection):
    query = connection._filter_to_query(["field", "between", 1, 10])

    assert query({"field": 5})
    assert not query({"field": 15})


def test_contains(connection):
    query = connection._filter_to_query(["field", "contains", "a"])

    assert query({"field": "cat"})
    assert not query({"field": "dog"})

    assert query({"field": ["a", "b", "c"]})
    assert not query({"field": ["d", "o", "g"]})


def test_ends_with(connection):
    query = connection._filter_to_query(["field", "ends_with", "z"])

    assert query({"field": "fizz"})
    assert not query({"field": "fuss"})


def test_greater_than(connection):
    query = connection._filter_to_query(["field", "greater_than", 5])

    assert query({"field": 10})
    assert not query({"field": 1})


def test_less_than(connection):
    query = connection._filter_to_query(["field", "less_than", 5])

    assert query({"field": 1})
    assert not query({"field": 10})


def test_in(connection):
    query = connection._filter_to_query(["field", "in", ["value"]])

    assert query({"field": "value"})
    assert not query({"field", "foobar"})
    assert not query({})


@pytest.mark.parametrize(
    "filter_value,pos_value,neg_value",
    (
        [(0, "DAY"), datetime.date(2000, 9, 2), datetime.date(2000, 9, 3)],
        [(1, "DAY"), datetime.date(2000, 9, 3), datetime.date(2000, 9, 2)],
        [(-1, "DAY"), datetime.date(2000, 9, 1), datetime.date(2000, 9, 2)],
        [(0, "MONTH"), datetime.date(2000, 9, 2), datetime.date(2000, 10, 2)],
        [(1, "MONTH"), datetime.date(2000, 10, 31), datetime.date(2000, 8, 31)],
        [(-1, "MONTH"), datetime.date(2000, 8, 1), datetime.date(2000, 10, 31)],
        [(0, "WEEK"), datetime.date(2000, 9, 2), datetime.date(2000, 9, 11)],
        [(1, "WEEK"), datetime.date(2000, 9, 9), datetime.date(2000, 9, 2)],
        [(-1, "WEEK"), datetime.date(2000, 8, 25), datetime.date(2000, 9, 2)],
        [(0, "YEAR"), datetime.date(2000, 9, 2), datetime.date(1999, 9, 2)],
        [(1, "YEAR"), datetime.date(2001, 9, 9), datetime.date(1999, 9, 2)],
        [(-1, "YEAR"), datetime.date(1999, 8, 30), datetime.date(2001, 9, 2)],
    ),
    ids=[
        "Today",
        "Yesterday",
        "Tomorrow",
        "This Month",
        "Last Month",
        "Next Month",
        "This Week",
        "Last Week",
        "Next Week",
        "This Year",
        "Last Year",
        "Next Year",
    ],
)
def test_in_calendar(connection, filter_value, pos_value, neg_value, mock_today):
    today = tinysg.utils.today()
    query = connection._filter_to_query(["field", "in_calendar", *filter_value])

    offset, unit = filter_value

    assert query({"field": pos_value}), f"Date {pos_value} is {offset} {unit}s from {today}"
    assert not query(
        {"field": neg_value}
    ), f"Date {pos_value} is not {offset} {unit}s from {today}"


def test_in_calendar_invalid_unit(connection):
    with pytest.raises(ValueError):
        query = connection._filter_to_query(["field", "in_calendar", 1, "COW"])
        query({"field": tinysg.utils.today()})


@pytest.mark.parametrize(
    "filter_value,value,expected",
    (
        [(3, "DAY"), datetime.date(2000, 9, 1), True],
        [(3, "DAY"), datetime.date(2000, 8, 1), False],
        [(3, "DAY"), datetime.date(2000, 10, 1), False],
        [(3, "WEEK"), datetime.date(2000, 8, 30), True],
        [(3, "WEEK"), datetime.date(2000, 8, 1), False],
        [(3, "WEEK"), datetime.date(2001, 10, 1), False],
        [(3, "MONTH"), datetime.date(2000, 8, 1), True],
        [(3, "MONTH"), datetime.date(2000, 1, 1), False],
        [(3, "MONTH"), datetime.date(2001, 1, 1), False],
        [(3, "YEAR"), datetime.date(1999, 9, 1), True],
        [(3, "YEAR"), datetime.date(1990, 9, 1), False],
        [(3, "YEAR"), datetime.date(2001, 9, 1), False],
    ),
    ids=[
        "In last 3 days",
        "Not in last 3 days (too early)",
        "Not in last 3 days (too late)",
        "In last 3 months",
        "Not in last 3 months (too early)",
        "Not in last 3 months (too late)",
        "In last 3 weeks",
        "Not in last 3 weeks (too early)",
        "Not in last 3 weeks (too late)",
        "In last 3 years",
        "Not in last 3 years (too early)",
        "Not in last 3 years (too late)",
    ],
)
def test_in_last(connection, filter_value, value, expected, mock_today):
    today = tinysg.utils.today()
    query = connection._filter_to_query(["field", "in_last", *filter_value])

    offset, unit = filter_value

    outcome = "is not" if expected else "is"
    actual = query({"field": value})

    assert actual == expected, f"Date {value} {outcome} in the last {offset} {unit}s of {today}"


def test_in_last_invalid_unit(connection):
    with pytest.raises(ValueError):
        query = connection._filter_to_query(["field", "in_last", 1, "COW"])
        query({"field": tinysg.utils.today()})


def test_in_last_invalid_value(connection):
    with pytest.raises(ValueError):
        query = connection._filter_to_query(["field", "in_last", -1, "DAY"])
        query({"field": tinysg.utils.today()})


@pytest.mark.parametrize(
    "filter_value,value,expected",
    (
        [(3, "DAY"), datetime.date(2000, 9, 5), True],
        [(3, "DAY"), datetime.date(2000, 9, 1), False],
        [(3, "DAY"), datetime.date(2000, 9, 6), False],
        [(3, "WEEK"), datetime.date(2000, 9, 23), True],
        [(3, "WEEK"), datetime.date(2000, 9, 1), False],
        [(3, "WEEK"), datetime.date(2001, 10, 15), False],
        [(3, "MONTH"), datetime.date(2000, 11, 1), True],
        [(3, "MONTH"), datetime.date(2000, 8, 1), False],
        [(3, "MONTH"), datetime.date(2001, 3, 1), False],
        [(3, "YEAR"), datetime.date(2003, 9, 1), True],
        [(3, "YEAR"), datetime.date(1999, 12, 31), False],
        [(3, "YEAR"), datetime.date(2005, 1, 11), False],
    ),
    ids=[
        "In next 3 days",
        "Not in next 3 days (too early)",
        "Not in next 3 days (too late)",
        "In next 3 months",
        "Not in next 3 months (too early)",
        "Not in next 3 months (too late)",
        "In next 3 weeks",
        "Not in next 3 weeks (too early)",
        "Not in next 3 weeks (too late)",
        "In next 3 years",
        "Not in next 3 years (too early)",
        "Not in next 3 years (too late)",
    ],
)
def test_in_next(connection, filter_value, value, expected, mock_today):
    today = tinysg.utils.today()
    query = connection._filter_to_query(["field", "in_next", *filter_value])

    offset, unit = filter_value

    outcome = "is not" if expected else "is"
    actual = query({"field": value})

    assert actual == expected, f"Date {value} {outcome} in the next {offset} {unit}s of {today}"


def test_in_next_invalid_unit(connection):
    with pytest.raises(ValueError):
        query = connection._filter_to_query(["field", "in_next", 1, "COW"])
        query({"field": tinysg.utils.today()})


def test_in_next_invalid_value(connection):
    with pytest.raises(ValueError):
        query = connection._filter_to_query(["field", "in_next", -1, "DAY"])
        query({"field": tinysg.utils.today()})


def test_is(connection):
    query = connection._filter_to_query(["field", "is", "value"])

    assert query({"field": "value"})
    assert not query({"field", "foobar"})
    assert not query({})


def test_is_not(connection):
    query = connection._filter_to_query(["field", "is_not", "value"])

    assert query({"field": "foobar"})
    assert not query({})
    assert not query({"field", "value"})


def test_not_between(connection):
    query = connection._filter_to_query(["field", "not_between", 1, 10])

    assert query({"field": 15})
    assert not query({"field": 5})


def test_not_contains(connection):
    query = connection._filter_to_query(["field", "not_contains", "a"])

    assert query({"field": "dog"})
    assert not query({"field": "cat"})

    assert query({"field": ["d", "o", "g"]})
    assert not query({"field": ["c", "a", "t"]})


def test_not_ends_with(connection):
    query = connection._filter_to_query(["field", "not_ends_with", "z"])

    assert query({"field": "fuss"})
    assert not query({"field": "fizz"})


def test_not_in(connection):
    query = connection._filter_to_query(["field", "not_in", ["value"]])

    assert query({"field": "foobar"})
    assert query({"field": ["foobar"]})
    assert not query({})
    assert not query({"field", "value"})


@pytest.mark.parametrize(
    "filter_value,pos_value,neg_value",
    (
        [(0, "DAY"), datetime.date(2000, 9, 3), datetime.date(2000, 9, 2)],
        [(1, "DAY"), datetime.date(2000, 9, 4), datetime.date(2000, 9, 3)],
        [(-1, "DAY"), datetime.date(2000, 9, 2), datetime.date(2000, 9, 1)],
        [(0, "MONTH"), datetime.date(2000, 10, 3), datetime.date(2000, 9, 10)],
        [(1, "MONTH"), datetime.date(2000, 9, 29), datetime.date(2000, 10, 3)],
        [(-1, "MONTH"), datetime.date(2000, 9, 2), datetime.date(2000, 8, 3)],
        [(0, "WEEK"), datetime.date(2000, 9, 15), datetime.date(2000, 9, 5)],
        [(1, "WEEK"), datetime.date(2000, 9, 2), datetime.date(2000, 9, 15)],
        [(-1, "WEEK"), datetime.date(2000, 9, 25), datetime.date(2000, 8, 25)],
        [(0, "YEAR"), datetime.date(2001, 9, 2), datetime.date(2000, 2, 9)],
        [(1, "YEAR"), datetime.date(2000, 2, 9), datetime.date(2001, 9, 2)],
        [(-1, "YEAR"), datetime.date(2001, 8, 30), datetime.date(1999, 9, 2)],
    ),
    ids=[
        "Not Today",  # Satan!
        "Not Yesterday",
        "Not Tomorrow",
        "Not This Month",
        "Not Last Month",
        "Not Next Month",
        "Not This Week",
        "Not Last Week",
        "Not Next Week",
        "Not This Year",
        "Not Last Year",
        "Not Next Year",
    ],
)
def test_not_in_calendar(connection, filter_value, pos_value, neg_value, mock_today):
    today = tinysg.utils.today()
    query = connection._filter_to_query(["field", "not_in_calendar", *filter_value])

    offset, unit = filter_value

    assert query({"field": pos_value}), f"Date {pos_value} is {offset} {unit}s from {today}"
    assert not query(
        {"field": neg_value}
    ), f"Date {neg_value} is not {offset} {unit}s from {today}"


def test_not_in_last(connection):
    pass


def test_not_in_next(connection):
    pass


def test_not_starts_with(connection):
    query = connection._filter_to_query(["field", "not_starts_with", "a"])

    assert query({"field": "beth"})
    assert not query({"field": "alice"})


def test_starts_with(connection):
    query = connection._filter_to_query(["field", "starts_with", "a"])

    assert query({"field": "alice"})
    assert not query({"field": "beth"})


def test_type_is(connection):
    query = connection._filter_to_query(["field", "type_is", "Asset"])

    assert query({"field": {"type": "Asset"}})
    assert not query({"field": {"type": "Shot"}})

    query = connection._filter_to_query(["field", "type_is", ["Asset", "Shot"]])

    assert query({"field": {"type": "Asset"}})
    assert query({"field": {"type": "Shot"}})
    assert not query({"field": {"type": "Sequence"}})


def test_type_is_not(connection):
    query = connection._filter_to_query(["field", "type_is_not", "Asset"])

    assert not query({"field": {"type": "Asset"}})
    assert query({"field": {"type": "Shot"}})

    query = connection._filter_to_query(["field", "type_is_not", ["Asset", "Shot"]])

    assert not query({"field": {"type": "Asset"}})
    assert not query({"field": {"type": "Shot"}})
    assert query({"field": {"type": "Sequence"}})
