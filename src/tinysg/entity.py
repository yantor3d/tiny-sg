"""Entity functions."""

import itertools
import enum

from typing import Mapping, List


class Fields(enum.Enum):
    """Entity field enums."""

    CODE = "code"
    ID = "id"
    NAME = "name"
    TYPE = "type"
    PROJECT = "project"


def as_entity_map(entity_list: List[dict]) -> Mapping[int, dict]:
    """Return the given entity list as an entity map."""

    return {entity[Fields.ID.value]: entity for entity in entity_list}


def as_handle(entity: dict) -> dict:
    """Return the entity handle for the given entity."""

    return _get(entity, [])


def eq(a: dict, b: dict) -> bool:
    """Return True if the two entities are the same."""

    return a["type"] == b["type"] and a["id"] == b["id"]


def get(entity_type: str, result: dict, return_fields: List[str] = None) -> dict:
    """Return the entity for the given tinydb Document."""

    entity = {
        Fields.ID.value: result.doc_id,
        Fields.TYPE.value: entity_type,
        **result,
    }

    return _get(entity, return_fields)


def null(entity_type: str) -> dict:
    """Return a null entity of the given type."""

    return {Fields.TYPE.value: entity_type, Fields.ID.value: -1}


def _get(entity: dict, return_fields: List[str] = None):
    """Return the payload for the given return fields."""

    default_fields = [
        Fields.TYPE.value,
        Fields.ID.value,
    ]

    if return_fields is None:
        return entity

    return {
        field: entity[field]
        for field in itertools.chain(default_fields, return_fields)
        if field in entity
    }
