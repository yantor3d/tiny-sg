"""Query filters."""

from __future__ import annotations

import functools
import collections
import datetime
import dateutil.relativedelta
import operator

import tinysg.utils

from enum import Enum
from typing import Callable, List, Union
from tinysg.exceptions import FilterSpecError

DateOrDateTime = Union[datetime.date, datetime.datetime]
FilterSpec = collections.namedtuple("FilterSpec", "field,op,value")


class CalendarUnits(Enum):
    """Calendar units."""

    DAY = "DAY"
    MONTH = "MONTH"
    WEEK = "WEEK"
    YEAR = "YEAR"


class FilterOperator(Enum):
    """Filter operator enums."""

    BETWEEN = "between"
    CONTAINS = "contains"
    ENDS_WITH = "ends_with"
    GREATER_THAN = "greater_than"
    IN = "in"
    IN_CALENDAR = "in_calendar"
    IN_LAST = "in_last"
    IN_NEXT = "in_next"
    IS = "is"
    IS_NOT = "is_not"
    LESS_THAN = "less_than"
    NOT_BETWEEN = "not_between"
    NOT_CONTAINS = "not_contains"
    NOT_ENDS_WITH = "not_ends_with"
    NOT_IN = "not_in"
    NOT_IN_CALENDAR = "not_in_calendar"
    NOT_IN_LAST = "not_in_last"
    NOT_IN_NEXT = "not_in_next"
    NOT_STARTS_WITH = "not_starts_with"
    STARTS_WITH = "starts_with"
    TYPE_IS = "type_is"
    TYPE_IS_NOT = "type_is_not"


FILTER_OPERATORS = {}


def neg(func):
    """Negate the wrapped function."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return not func(*args, **kwargs)

    return wrapper


def register(*fops: str) -> Callable:
    """Register the decorated function to the given filter operators."""

    def inner(func):
        for fop in fops:
            if fop.startswith("not_"):
                FILTER_OPERATORS[fop] = neg(func)
            elif fop.endswith("_not"):
                FILTER_OPERATORS[fop] = neg(func)
            else:
                FILTER_OPERATORS[fop] = func
        return func

    return inner


def parse_filter_spec(filter_spec: List) -> FilterSpec:
    """Parse the given filter spec is valid.

    Args:
        filter_spec (List): Filter spec - a field, an operator, and a value.

    Raises:
        tinysg.exceptions.FilterSpecError: If the given filter spec is not the right shape.

    Returns:
        tinysg.filters.FilterSpec
    """

    if len(filter_spec) not in [3, 4]:
        raise FilterSpecError(
            f"Invalid filter spec {filter_spec} - expected [field, operator, value(s)]"
        )

    field, filter_op, *filter_value = filter_spec

    return FilterSpec(field, filter_op, filter_value)


def get(filter_op) -> Callable:
    """Return the query for the given filter_spec.

    Args:
        filter_op (str): Filter operator spec.

    Raises:
        tinysg.exceptions.FilterSpecError: If the given filter op is not supported.

    Returns:
        callable
    """

    try:
        return FILTER_OPERATORS[filter_op]
    except KeyError:
        filter_ops_list = ", ".join(FILTER_OPERATORS.keys())
        raise FilterSpecError(
            f"Invalid filter operator: '{filter_op} - expected: {filter_ops_list}."
        )


@register(
    FilterOperator.BETWEEN.value,
    FilterOperator.NOT_BETWEEN.value,
)
def between(a, mn, mx) -> bool:
    """Return True if 'a' is between 'mn' and 'mx'.

    Args:
        a (Any): Value to test.
        mn (Any): Minimum value to test against.
        mx (Any): Maxmimum value to test against.

    Returns:
        bool
    """

    return mn < a and a < mx


@register(
    FilterOperator.CONTAINS.value,
    FilterOperator.NOT_CONTAINS.value,
)
def contains(a, b) -> bool:
    """Return True if 'a' contains 'b'."""

    return operator.contains(a, b)


register(
    FilterOperator.ENDS_WITH.value,
    FilterOperator.NOT_ENDS_WITH.value,
)(str.endswith)


register(
    FilterOperator.GREATER_THAN.value,
)(operator.gt)


@register(
    FilterOperator.IN_CALENDAR.value,
    FilterOperator.NOT_IN_CALENDAR.value,
)
def in_calendar(value: DateOrDateTime, n: int, unit: str) -> bool:
    """Return True if 'value' is on the 'n' calendar unit from today.

    Args:
        value (datetime.date | datetime.datetime): Date/Datetime to test.
        n (int): Offset in time units (0 = today, 1 = next, -1 = last)
        units (str): Units of time (DAY, WEEK, MONTH, or YEAR)

    Raises:
        ValueError: If 'units' is not 'DAY', 'WEEK', 'MONTH', or 'YEAR'

    Returns:
        bool
    """

    today = tinysg.utils.today()

    if unit == CalendarUnits.MONTH.value:
        # relativedelta does not correctly express the idea of
        # "last month" or "next month" for dates during the month.
        delta = dateutil.relativedelta.relativedelta(
            dt1=datetime.date(value.year, value.month, 1),
            dt2=datetime.date(today.year, today.month, 1),
        )
    else:
        delta = dateutil.relativedelta.relativedelta(
            dt1=value,
            dt2=today,
        )

    try:
        return getattr(delta, unit.lower() + "s") == n
    except AttributeError:
        raise ValueError(unit)


@register(
    FilterOperator.IN_LAST.value,
    FilterOperator.NOT_IN_LAST.value,
)
def in_last(value: DateOrDateTime, n: int, unit: str) -> bool:
    """Return True if 'date' is within the last N units.

    Args:
        value (datetime.date | datetime.datetime): Date/Datetime to test.
        n (int): Number of hours/days/weeks/months/years
        unit (str): Units of time (HOUR, DAY, WEEK, MONTH, or YEAR)

    Raises:
        ValueError: If 'units' is not 'HOUR', 'DAY', 'WEEK', 'MONTH', or 'YEAR'
        ValueError: If 'n' is less than 0.

    Returns:
        bool
    """

    if n < 0:
        raise ValueError("Number of {unit.lower()}s must be greater than zero.")

    today = tinysg.utils.today()
    key = unit.lower() + "s"

    try:
        delta = dateutil.relativedelta.relativedelta(**{key: n})
    except TypeError:
        raise ValueError("Unknown unit of time: {unit}")

    return today - delta <= value <= today


@register(
    FilterOperator.IN_NEXT.value,
    FilterOperator.NOT_IN_NEXT.value,
)
def in_next(value: Union[datetime.date, datetime.datetime], n: int, unit: str) -> bool:
    """Return True if 'date' is within the next N units.

    Args:
        value (datetime.date | datetime.datetime): Date/Datetime to test.
        n (int): Number of hours/days/weeks/months/years
        unit (str): Units of time (HOUR, DAY, WEEK, MONTH, or YEAR)

    Raises:
        ValueError: If 'units' is not 'HOUR', 'DAY', 'WEEK', 'MONTH', or 'YEAR'
        ValueError: If 'n' is less than 0.

    Returns:
        bool
    """

    if n < 0:
        raise ValueError("Number of {unit.lower()}s must be greater than zero.")

    today = tinysg.utils.today()
    key = unit.lower() + "s"

    try:
        delta = dateutil.relativedelta.relativedelta(**{key: n})
    except TypeError:
        raise ValueError("Unknown unit of time: {unit}")

    return today <= value <= today + delta


@register(
    FilterOperator.IN.value,
    FilterOperator.NOT_IN.value,
)
def in_(a, b):
    """Return True if 'a' is in 'b'."""

    if isinstance(a, (list, tuple)):
        return any([a_ in b for a_ in a])
    else:
        return a in b


register(
    FilterOperator.IS.value,
    FilterOperator.IS_NOT.value,
)(operator.eq)


register(
    FilterOperator.LESS_THAN.value,
)(operator.lt)


register(
    FilterOperator.STARTS_WITH.value,
    FilterOperator.NOT_STARTS_WITH.value,
)(str.startswith)


@register(
    FilterOperator.TYPE_IS.value,
    FilterOperator.TYPE_IS_NOT.value,
)
def type_is(entity: dict, entity_type: Union[str, List[str]]) -> bool:
    """Return True if 'entity' is the given entity type.

    Args:
        entity (dict): Entity to test.
        entity_type (str | list[str]): Entity type(s) to test.

    Returns:
        bool
    """

    if isinstance(entity_type, str):
        return entity["type"] == entity_type
    else:
        return entity["type"] in entity_type
