"""Package utilities."""

import collections
import datetime

from typing import Dict, List, Tuple


def as_key(entity: dict) -> Tuple[str, int]:
    """Return the given entity as a key.

    Args:
        entity (dict): Database entity.

    Returns:
        tuple[str, int]
    """

    return (entity["type"], entity["id"])


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


def today() -> datetime.date:
    """Return today's date."""

    return datetime.date.today()
