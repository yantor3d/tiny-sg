"""Package utilities."""

import collections
import datetime

from typing import Any, Dict, List, Tuple, Union


def as_key(entity: dict) -> Tuple[str, int]:
    """Return the given entity as a key.

    Args:
        entity (dict): Database entity.

    Returns:
        tuple[str, int]
    """

    return (entity["type"], entity["id"])


def first(items: List[Any]) -> Union[Any, None]:
    """Return the first item in the given list."""

    return next(iter(items), None)


def group_by_type(entity_list: List[dict]) -> Dict[str, List[dict]]:
    """Return the list of entities, grouped by type.

    Args:
        entity_list (list[dict]): List of entities to group.

    Returns:
        dict[str, list[dict]]
    """

    result = collections.defaultdict(list)

    for entity in entity_list:
        result[entity["type"]].append(entity)

    return result


def now() -> datetime.datetime:
    """Return the current date and time."""

    return datetime.datetime.now()


def reindex(table: Dict[str, str]) -> Dict[str, str]:
    """Re-index the given table so its keys are contiguous.

    Args:
        table (dict): An entity table; keys are assumed to be numbers.

    Returns:
        dict
    """

    return {str(i): value for i, (__, value) in enumerate(sorted(table.items()), 1)}


def today() -> datetime.date:
    """Return today's date."""

    return datetime.date.today()
