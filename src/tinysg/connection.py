"""Database connection."""

import collections
import logging
import os

from typing import List, Mapping, Optional
from tinydb import TinyDB, JSONStorage, Query, where
from tinydb.table import Document

import tinysg.entity
import tinysg.fields
import tinysg.filters
import tinysg.operations
import tinysg.utils

from tinysg.entity import Fields
from tinysg.fields import FieldType, UpdateMode
from tinysg.exceptions import EntityNotFound, FilterSpecError, SchemaError
from tinysg.middleware import PivotTableMiddleware, ReadCachingMiddleware

__all__ = ["Connection"]


class Connection(object):
    """TSG connection.

    Provides the CRUD methods to interact with the database.
    """

    def __init__(self, path: str):
        """Initialize.

        Args:
            path (str): Filepath of the database file.

        Raises:
            FileNotFoundError: If the given database file does not exist.
        """

        if not os.path.exists(path):
            raise FileNotFoundError(f"Given path {path} does not exist!")

        self.log = logging.getLogger(__name__ + "." + self.__class__.__name__)

        self._db = TinyDB(
            path,
            storage=ReadCachingMiddleware(
                PivotTableMiddleware(JSONStorage),
            ),
        )

    @property
    def __tables(self) -> dict:
        """Return a 'pointer' to the raw db tables."""

        return self._db.storage.cache

    def create(self, entity_type: str, data: dict, return_fields: List[str] = None) -> dict:
        """Create a new entity of the using the given data.

        Args:
            entity_type (str): Type of entity to return.
            data (dict): Entity field values.
            return_fields (list[str]): List of fields to return.

        Raises:
            SchemaError: If the given entity type does not exist.
            SchemaError: If a value for a field that does not exist is provided.
            ValueError: If an invalid value is provided.
            ValueError: If a required field is not provided, or is null.

        Returns:
            dict
        """

        payload = self.__get_entity_payload(entity_type, data)

        missing_fields = self.__check_entity_payload(entity_type, payload)

        if missing_fields:
            raise ValueError(
                f"Must set required fields for '{entity_type}' entity: {', '.join(missing_fields)}'"
            )

        non_unique_fields = self.__check_entity_identifier(entity_type, payload)

        if non_unique_fields:
            raise ValueError(
                f"Cannot create '{entity_type}' entity "
                f"because its identifier field values are not unique: {', '.join(non_unique_fields)}"
            )

        table = self._db.table(entity_type)
        entity_id = table._get_next_id()
        payload["id"] = int(entity_id)

        table.insert(Document(payload, doc_id=entity_id))

        result = self.__get_entity(entity_type, entity_id, return_fields)

        self._db.storage.flush()

        return result

    def delete(self, entity_type: str, entity_id: int) -> bool:
        """Delete the given entity.

        Args:
            entity_type (str): Type of entity to delete.
            entity_id (id): ID of the entity to delete.

        Raises:
            SchemaError: If the given entity type does not exist.
            EntityNotFound: If the given entity does not exist.

        Returns:
            bool: True if the entity was deleted, False if the entity was already deleted.
        """

        self.schema_entity_read(entity_type)

        active_entity = self.__get_entity_raw(entity_type, entity_id, retired=False) or None
        retire_entity = self.__get_entity_raw(entity_type, entity_id, retired=True) or None

        if active_entity is None and retire_entity is None:
            raise EntityNotFound(
                "A(n) 'f{entity_type}' entity for id f{entity_id} does not exist."
            )
        elif active_entity is not None:
            entity = self.__get_entity_raw(entity_type, entity_id, retired=False)
            entity_view = dict(entity)

            fields = self.schema_field_read_all(entity_type)

            for field in fields:
                if field["type"] == "entity":
                    self.__update_entity_link_field(
                        entity_view,
                        field,
                        value=None,
                    )
                elif field["type"] == "multi_entity":
                    self.__update_multi_entity_link_field(
                        entity_view,
                        field,
                        value=[],
                        update_mode=UpdateMode.SET.value,
                    )

            self.__set_entity_raw(entity_type, entity_id, {}, retired=False)
            self.__set_entity_raw(entity_type, entity_id, dict(entity), retired=True)

            self._db.storage.flush()

            result = True
        elif retire_entity is not None:
            result = False

        return result

    def find_one(
        self,
        entity_type: str,
        filters: List,
        return_fields: List[str] = None,
        retired_only: bool = False,
    ) -> Optional[dict]:
        """Return the first matching record for the given entity type.

        Args:
            entity_type (str): Type of entity to return.
            filters (list): List of filters for the query.
            return_fields (list[str]): List of fields to return.
            retired_only (bool): It True, only return retired entities.

        Returns:
            dict | None
        """

        results = self.find_all(entity_type, filters, return_fields, retired_only)

        return next(iter(results), None)

    def find_all(
        self,
        entity_type: str,
        filters: List,
        return_fields: List[str] = None,
        retired_only: bool = False,
    ) -> List[dict]:
        """Return all matching records for the given entity type.

        Args:
            entity_type (str): Type of entity to query.
            filters (list): List of filters for the query.
            return_fields (list[str]): List of fields to return.
            retired_only (bool): It True, only return retired entities.

        Returns:
            list[dict]
        """

        self.schema_entity_read(entity_type)

        table_name = self.__get_table_name(entity_type, retired_only)

        if filters:
            filters = self._resolve_filters(entity_type, filters)
            query = self._filters_to_query(filters)
            results = self._db.table(table_name).search(query)
        else:
            results = self._db.table(table_name).all()

        # TODO: Return empty list for requested multi-entity return fields with no links
        results = [tinysg.entity.get(entity_type, result, return_fields) for result in results]
        results = self._join_linked_entities(results, return_fields)

        return results

    def revive(self, entity_type: str, entity_id: int) -> bool:
        """Revive the given entity.

        Args:
            entity_type (str): Type of entity to revive.
            entity_id (id): ID of the entity to revive.

        Raises:
            SchemaError: If the given entity type does not exist.
            EntityNotFound: If the given entity does not exist.

        Returns:
            bool: True if the entity was revived, False if the entity was already revived.
        """

        self.schema_entity_read(entity_type)

        active_entity = self.__get_entity_raw(entity_type, entity_id, retired=False) or None
        retire_entity = self.__get_entity_raw(entity_type, entity_id, retired=True) or None

        if active_entity is None and retire_entity is None:
            raise EntityNotFound(
                "A(n) 'f{entity_type}' entity for id f{entity_id} does not exist."
            )
        elif active_entity is not None:
            result = False
        elif retire_entity is not None:
            entity = retire_entity

            fields = self.schema_field_read_all(entity_type)

            # Remove retired links
            for field in fields:
                if field["type"] == "entity":
                    link = retire_entity.get(field["name"])

                    if link is None:
                        continue
                    elif not self.__has_entity(link["type"], link["id"]):
                        retire_entity.pop(field["name"], None)
                elif field["type"] == "multi_entity":
                    links = retire_entity.get(field["name"], [])
                    links = [link for link in links if self.__has_entity(link["type"], link["id"])]

                    if links:
                        entity[field["name"]] = links
                    else:
                        entity.pop(field["name"], None)

            self.__set_entity_raw(entity_type, entity_id, entity, retired=False)
            self.__set_entity_raw(entity_type, entity_id, None, retired=True)

            self._db.storage.flush()

            result = True

        return result

    def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict,
        multi_entity_update_modes: dict = None,
    ) -> dict:
        """Update the fields on the given entity.

        Args:
            entity_type (str): Type of entity to update.
            entity_id (int): ID of the entity to update.
            data (dict[str, Any]): Field values to update.
            multi_entity_update_modes (dict[str, str]): Update mode to use when updating a multi-entity link field.
                They keys of the dict are the fields to set the mode for. The values from the dict are one of 'set',
                'add', or 'remove'. The default is 'set'.

        Raises:
            EntityNotFound: If the given entity does not exist.
            SchemaError: If the given entity type does not exist.
            SchemaError: If a given field does not exist.
            ValueError: If an invalid value is provided.
            ValueError: If an invalid update mode is given for a multi-entity field.
            ValueError: If a required field is not provided, or is null.

        Returns:
            dict
        """

        self.schema_entity_read(entity_type)

        table = self._db.table(entity_type)

        entity = table.get(doc_id=entity_id)

        if entity is None:
            raise EntityNotFound(
                "A(n) 'f{entity_type}' entity for id f{entity_id} does not exist."
            )

        # __get_entity_payload drops nulls - update the original data to preserve them
        payload = dict(data)
        payload.update(self.__get_entity_payload(entity_type, data, payload={}))

        missing_fields = self.__check_entity_payload(entity_type, payload)
        missing_fields = [field for field in missing_fields if field in data]

        if missing_fields:
            raise ValueError(
                f"Cannot unset required fields for '{entity_type}' entity: {', '.join(missing_fields)}'"
            )

        old_payload = dict(entity)
        old_payload.update(payload)

        non_unique_fields = self.__check_entity_identifier(entity_type, old_payload, entity_id)
        non_unique_fields = [field for field in non_unique_fields if field in data]

        if non_unique_fields:
            raise ValueError(
                f"Cannot update '{entity_type}' ({entity_id}) "
                f"because its new identifier field values are not unqiue: {', '.join(non_unique_fields)}"
            )

        fields = self.schema_field_read_all(entity_type)
        fields_map = {field["name"]: field for field in fields}
        multi_entity_update_modes = multi_entity_update_modes or {}

        self.__validate_multi_entity_update_modes(
            entity_type, fields_map, multi_entity_update_modes
        )

        # tinydb can only update one table at a time, and a multi-entity field
        # may have a "reverse field" that also needs to be updated.
        # If we go through the API, an edit may get lost because of how our
        # middleware transforms the data for persistence.
        entity = self.__get_entity_raw(entity_type, entity_id)

        for field_name, value in payload.items():
            field = fields_map[field_name]

            if field["type"] == "entity":
                self.__update_entity_link_field(
                    entity,
                    field,
                    value or None,
                )
            elif field["type"] == "multi_entity":
                self.__update_multi_entity_link_field(
                    entity,
                    field,
                    value or [],
                    multi_entity_update_modes.get(field_name),
                )
            elif value is None:
                entity.pop(field_name, None)
            else:
                entity[field_name] = value

        result = self.__get_entity(entity_type, entity_id, return_fields=list(data.keys()))

        self._db.storage.flush()

        return result

    def __update_entity_link_field(
        self,
        entity: dict,
        field: dict,
        value: Optional[dict],
    ):
        """Update the given entity field and its reverse fields.

        Args:
            entity (dict): Entity to update the field on.
            field (int): Field spec of the field being updated.
            value (dict | None): Update value for the field.
        """

        field_name = field["name"]

        old_value = entity.get(field_name, None)
        new_value = value or None

        if new_value:
            entity[field_name] = new_value
        else:
            entity.pop(field_name, None)

        entity = tinysg.entity.as_handle(entity)

        reverse_fields = self._db.table("_fields").search(
            (where("table") == field["table"]) & (where("link") == entity["type"])
        )

        for reverse_field in reverse_fields:
            field_name = reverse_field["name"]

            old_links = [old_value] if old_value else []
            new_links = [new_value] if new_value else []

            self.__unlink_entity_from(entity, reverse_field, old_links, new_links)

    def __update_multi_entity_link_field(
        self,
        entity: dict,
        field: dict,
        value: list[dict],
        update_mode: str,
    ):
        """Update the given multi-entity field and its reverse fields.

        Args:
            entity (dict): Entity to update the field on.
            field (int): Field spec of the field being updated.
            value (list[dict]): Update value(s) for the field.
            update_mode (str): Update mode for the field.
        """

        field_name = field["name"]

        old_values = entity.get(field_name, [])
        new_values = tinysg.fields.update_multi_entity_field(
            old_values=old_values,
            new_values=value,
            update_mode=update_mode,
        )

        if new_values:
            entity[field_name] = new_values
        else:
            entity.pop(field_name, None)

        entity = tinysg.entity.as_handle(entity)

        old_links_map = tinysg.utils.group_by_type(old_values)
        new_links_map = tinysg.utils.group_by_type(new_values)

        reverse_fields = self._db.table("_fields").search(
            (where("table") == field["table"]) & (where("link") == entity["type"])
        )

        for reverse_field in reverse_fields:
            field_name = reverse_field["name"]
            entity_type = reverse_field["entity_type"]

            old_links = old_links_map.get(entity_type, [])
            new_links = new_links_map.get(entity_type, [])

            self.__unlink_entity_from(entity, reverse_field, old_links, new_links)

    def __unlink_entity_from(
        self, entity: dict, reverse_field: dict, old_links: List[dict], new_links: List[dict]
    ) -> None:
        """Unlink the given entity from the reverse field on the given links.

        Args:
            entity (dict): Entity to unlink from the reverse field.
            reverse_field (dict): Spec for the reverse field.
            old_links (dict): Entities that link the given entity before the update.
            old_links (dict): Entities that link the given entity after the update.
        """

        field_name = reverse_field["name"]

        old_links_ids = {each["id"] for each in old_links}
        new_links_ids = {each["id"] for each in new_links}

        for link in old_links:
            if link["id"] in new_links_ids:
                continue

            link = self.__get_entity_raw(link["type"], link["id"])

            if reverse_field["type"] == "multi_entity":
                old_value = link.get(field_name, [])
                new_value = [each for each in old_value if each != entity]

                link[field_name] = new_value
            elif reverse_field["type"] == "entity":
                link.pop(field_name, None)

        for link in new_links:
            if link["id"] in old_links_ids:
                continue

            link = self.__get_entity_raw(link["type"], link["id"])

            if reverse_field["type"] == "multi_entity":
                old_value = link.get(field_name, [])
                new_value = old_value[:]
                new_value.append(entity)

    def __has_entity(self, entity_type: str, entity_id: int, retired=False) -> bool:
        """Return True if the given entity exists."""

        table_name = self.__get_table_name(entity_type, retired)

        return bool(self.__tables.get(table_name, {}).get(str(entity_id)))

    def __get_entity_raw(self, entity_type: str, entity_id: int, retired=False) -> dict:
        """Return a raw handle to the given entity."""

        table_name = self.__get_table_name(entity_type, retired)

        return self.__tables.get(table_name, {}).get(str(entity_id))

    def __set_entity_raw(
        self, entity_type: str, entity_id: int, data: dict, retired=False
    ) -> dict:
        """Set the raw data of the given entity."""

        table_name = self.__get_table_name(entity_type, retired)

        if data is None:
            self.__tables.setdefault(table_name, {}).pop(str(entity_id), None)
        else:
            self.__tables.setdefault(table_name, {})[str(entity_id)] = data

    def __validate_multi_entity_update_modes(
        self,
        entity_type: str,
        fields_map: dict,
        multi_entity_update_modes: dict,
    ) -> None:
        """Validate the multi-entity update modes."""

        update_modes = UpdateMode.values()
        update_modes_str = ", ".join(update_modes)

        for field_name, update_mode in multi_entity_update_modes.items():
            field = fields_map[field_name]

            if field["type"] != FieldType.MULTI_ENTITY.value:
                raise ValueError(f"'{entity_type}.{field_name}' is not a multi-entity field.")

            if update_mode not in update_modes:
                raise ValueError(
                    f"Invalid update mode '{update_mode}' for multi-entity field '{entity_type}.{field_name}'"
                    f" - expected {update_modes_str}."
                )

    def _resolve_filters(
        self,
        entity_type: str,
        filters: List,
    ) -> List:
        """Resolve the deep links in the filters.

        Args:
            entity_type (str): Type of entity to query.
            filters (list): List of filters for the query.

        Returns:
            list
        """

        results = []

        link_filters = collections.defaultdict(list)
        link_types = dict()

        for filter_spec in filters:
            field, filter_op, filter_value = tinysg.filters.parse_filter_spec(filter_spec)

            if tinysg.fields.is_deep_field(field):
                entity_field, link_type, link_field = tinysg.fields.parse_deep_field(field)

                field_schema = self.schema_field_read(entity_type, entity_field)

                if not tinysg.fields.is_link(field_schema):
                    raise FilterSpecError(
                        f"Cannot do deep filter on non-link field {entity_type}.{entity_field}"
                    )

                link_filters[entity_field].append([link_field, filter_op, *filter_value])
                link_types[entity_field] = link_type
            else:
                self.schema_field_read(entity_type, field)

                results.append(filter_spec)

            # TODO: Check field/filter_op compatability

        fops = tinysg.filters.FilterOperator

        for link_field, link_filters in link_filters.items():
            link_type = link_types[link_field]
            links = self.find_all(link_type, link_filters)
            links = links or [tinysg.entity.null(link_type)]
            links = [tinysg.entity.as_handle(link) for link in links]

            if len(links) == 1:
                results.append([link_field, fops.IS.value, links[0]])
            else:
                results.append([link_field, fops.IN.value, links])

        return results

    def _filters_to_query(self, filters: List):
        """Return the tinydb query for the given filters.

        Args:
            filters (list): List of filters for the query.

        Returns:
            Query
        """

        result = None

        for filter_spec in filters:
            query = self._filter_to_query(filter_spec)

            if result is None:
                result = query
            else:
                result = result & query

        return result

    def _filter_to_query(self, filter_spec: List[str]) -> Query:
        """Return the tinydb query for the given filter spec.

        Args:
            filters_spec (list[str]): Filter spec to create the tinydb query for.

        Returns:
            Query
        """

        field, filter_op, filter_value = tinysg.filters.parse_filter_spec(filter_spec)
        func = tinysg.filters.get(filter_op)

        return Query()[field].test(lambda field_value: func(field_value, *filter_value))

    def _join_linked_entities(
        self,
        results: List[dict],
        return_fields: List[str] = None,
    ) -> List[dict]:
        """Return the entity form of the given tinydb record.

        Args:
            results (list[dict]): List of entities to join the linked fields on.
            return_fields (list[str]): List of entity fields to return.

        Returns:
            list[dict]
        """

        link_ids = self.__get_links_map(results)
        link_fields = self.__get_link_fields_map(return_fields)

        links = self.__get_linked_field_values(link_ids, link_fields)
        self.__set_linked_field_values(results, links)

        return results

    def __check_entity_identifier(
        self, entity_type: str, data: dict, entity_id: int = None
    ) -> bool:
        """Return the identifier fields of the entity if another same values already exists.

        Args:
            entity_type (str): Type of entity to check the payload for.
            data (dict): Entity payload to validate.
            entity_id (id): Entity ID of the entity the payload is for.

        Returns:
            list[str]
        """

        result = []

        identifier = {
            field["name"]: data.get(field["name"])
            for field in self.schema_field_read_all(entity_type)
            if field.get("identifier", False)
        }

        if identifier:
            filters = [[field, "is", value] for field, value in identifier.items()]
            entity = self.find_one(entity_type, filters)

            if (entity is not None) and (entity["id"] != entity_id):
                result = list(identifier.keys())
                result.sort()

        return result

    def __check_entity_payload(self, entity_type: str, data: dict) -> List[str]:
        """Return the missing required field(s) in the given entity payload.

        Args:
            entity_type (str): Type of entity to get the payload for.
            data (dict): Entity payload to validate.

        Returns:
            list[str]
        """

        fields = self.schema_field_read_all(entity_type)

        required_fields = [field for field in fields if field.get("required", False)]
        missing_fields = [
            field["name"] for field in required_fields if data.get(field["name"]) is None
        ]
        missing_fields.sort()

        return missing_fields

    def __get_links_map(self, entity_list: List[dict]) -> Mapping:
        """Return a map of the linked entities in the given results.

        Args:
            results [list[dict]]: List of entities returned by a query.

        Returns:
            dict[str, set[int]]
        """

        result = collections.defaultdict(set)

        def _add(link):
            result[link[Fields.TYPE.value]].add(link[Fields.ID.value])

        for entity in entity_list:
            entity_type = entity[Fields.TYPE.value]

            for field, value in entity.items():
                field_schema = self.schema_field_read(entity_type, field)

                if tinysg.fields.is_entity(field_schema):
                    _add(value)
                elif tinysg.fields.is_multi_entity(field_schema):
                    for val in value:
                        _add(val)

        return result

    def __get_link_fields_map(self, return_fields: List[str] = None) -> Mapping[str, List]:
        """Return a map of the link fields in the given return fields.

        Args:
            return_fields (list[str]): List of return fields for the query.

        Returns:
            dict[str, list]
        """

        result = collections.defaultdict(list)

        for field in return_fields or []:
            if tinysg.fields.is_deep_field(field):
                __, link_type, link_field = tinysg.fields.parse_deep_field(field)

                result[link_type].append(link_field)

        return result

    def __get_linked_field_values(self, link_ids, link_fields) -> Mapping[str, dict]:
        """Get the linked field values.

        Args:
            link_ids [dict[str, set[int]]]: Map of linked entity ids.
            link_fields [dict[str, list[str]]]: Map of linked deep fields.

        Returns:
            dict[str, dict[int, dict]]
        """

        result = {}

        for entity_type, entity_ids in link_ids.items():
            entity_fields = [Fields.CODE.value] + link_fields.get(entity_type, [])
            entity_list = self._db.table(entity_type).get(doc_ids=entity_ids)
            entity_list = [
                tinysg.entity.get(entity_type, each, entity_fields) for each in entity_list
            ]

            self._join_linked_entities(entity_list, entity_fields)

            for entity in entity_list:
                entity[Fields.NAME.value] = entity.pop(Fields.CODE.value)

            result[entity_type] = tinysg.entity.as_entity_map(entity_list)

        return result

    def __get_entity(
        self,
        entity_type: str,
        entity_id: int,
        return_fields: list[str] = None,
    ) -> dict:
        """Return the entity with the given id."""

        result = self._db.table(entity_type).get(doc_id=entity_id)
        result = tinysg.entity.get(entity_type, result, return_fields)
        (result,) = self._join_linked_entities([result], return_fields)

        return result

    def __get_entity_payload(self, entity_type: str, data: dict, payload=None) -> dict:
        """Return the entity payload for the given data.

        Args:
            entity_type (str): Type of entity to get the payload for.
            data (dict): Field/value data.
            payload (dict): Initial payload for the entity.

        Returns:
            dict
        """

        fields = self.schema_field_read_all(entity_type)
        fields_map = {field["name"]: field for field in fields}

        if payload is None:
            payload = {
                "type": entity_type,
                "id": None,
            }

        linked_entities = collections.defaultdict(list)

        for field_name, value in data.items():
            if field_name not in fields_map:
                raise SchemaError(f"The '{entity_type} schema has no '{field_name}', field.")

            field = fields_map[field_name]
            value = tinysg.fields.handle_value(value, field)

            if value is not None:
                payload[field_name] = value

                if field["type"] == "entity":
                    linked_entities[field["name"]] = [value]
                elif field["type"] == "multi_entity":
                    linked_entities[field["name"]] = value

        for field_name, links in sorted(linked_entities.items()):
            field = fields_map[field_name]
            link_ids = {link["id"] for link in links}

            links = self._db.table(field["link"]).get(doc_ids=list(link_ids))

            missing_link_ids = link_ids - {link.doc_id for link in links}

            if missing_link_ids:
                missing_link_ids_str = ", ".join(sorted(map(str, missing_link_ids)))
                raise EntityNotFound(
                    f"Cannot link '{field['link']}' entities to '{entity_type}.{field_name}'"
                    f"because they do not exist: {missing_link_ids_str}"
                )

        return payload

    def __get_table_name(self, entity_type: str, retired=False):
        """Return the table name for the given entity type."""

        if retired:
            return f"Retired:{entity_type}"
        else:
            return entity_type

    def __set_linked_field_values(self, results, links) -> None:
        """Set the values of the linked fields.

        Args:
            results [list[dict]]: List of entities returned by a query.
            links [dict]: Map of linked entities.
        """

        def _get(link):
            return links[link[Fields.TYPE.value]][link[Fields.ID.value]]

        for result in results:
            entity_type = result[Fields.TYPE.value]

            for field, value in result.items():
                field_schema = self.schema_field_read(entity_type, field)

                if tinysg.fields.is_entity(field_schema):
                    result[field] = _get(value)
                elif tinysg.fields.is_multi_entity(field_schema):
                    result[field] = [_get(v) for v in value]

        return results

    def schema_entity_check(self, entity_type: str) -> bool:
        """Return True if the given entity type is part of the schema..

        Args:
            entity_type (str): Entity type to return the schema for.

        Returns:
            bool
        """

        result = self._db.table("_schema").get(where("entity_type") == entity_type)

        return result is not None

    def schema_entity_create(self, entity_type: str) -> dict:
        """Add the given entity type to the schema.

        Args:
            entity_type (str): Name of the entity type to create.

        Raises:
            tinysg.exceptions.SchemaError: If the given entity schema already exists.

        Returns:
            dict
        """

        try:
            self.schema_entity_read(entity_type)
        except SchemaError:
            self._db.table("_schema").insert(
                {
                    "entity_type": entity_type,
                }
            )

            return self.schema_entity_read(entity_type)
        else:
            raise SchemaError(f"A(n) '{entity_type} entity has already been registered.")

    def schema_entity_read(self, entity_type: str) -> dict:
        """Return the schema for the given entity type.

        Args:
            entity_type (str): Entity type to return the schema for.

        Raises:
            tinysg.exceptions.SchemaError: If the given entity schema does not exist.

        Returns:
            dict
        """

        result = self._db.table("_schema").get(where("entity_type") == entity_type)

        if result is None:
            raise SchemaError(f"A(n) '{entity_type} entity has not been registered.")
        else:
            return dict(result)

    def schema_entity_read_all(self) -> dict:
        """Return the schema for all registered entity types.

        Returns:
            dict
        """

        return {result["entity_type"]: result for result in self._db.table("_schema").all()}

    def schema_field_check(
        self,
        entity_type: str,
        field_name: str,
    ) -> bool:
        """Return True if the given field is part of the schema.

        Args:
            entity_type (str): Entity type to check the for.
            field_name (str): Name of the field to check for.

        Returns:
            bool
        """

        result = self._db.table("_fields").get(
            (where("entity_type") == entity_type) & (where("name") == field_name)
        )

        return result is not None

    def schema_field_create(self, entity_type: str, field_name: str, properties: dict) -> dict:
        """Add a field to the given entity type in the schema.

        Args:
            entity_type (str): Name of the entity type to create.
            field_name (str): Name of the field to create.

        Raises:
            tinysg.exceptions.SchemaError: If the given entity schema does not exist.
            tinysg.exceptions.SchemaError: If the given field already exists.

        Returns:
            bool
        """

        try:
            self.schema_field_read(entity_type, field_name)
        except SchemaError:
            self.schema_entity_read(entity_type)

            tinysg.fields.validate_spec(field_name, properties)

            # TODO: Verify link entity exists
            # TODO: Create/update link entity table

            self._db.table("_fields").insert(
                {
                    "entity_type": entity_type,
                    "name": field_name,
                    **properties,
                }
            )

            return self.schema_field_read(entity_type, field_name)
        else:
            raise SchemaError(f"A(n) '{entity_type}.{field_name}' field already exists.")

    def schema_field_delete(self, entity_type: str, field_name: str) -> None:
        """Delete the given field the schema.

        Args:
            entity_type (str): Name of the entity type to delete the field on.
            field_name (str): Name of the field to delete

        Raises:
            tinysg.exceptions.SchemaError: If the given entity schema does not exist.
            tinysg.exceptions.SchemaError: If the given field does not exists.
        """

        field = self.schema_field_read(entity_type, field_name)

        self._db.table("_fields").remove(doc_ids=[field["id"]])
        self._db.table(entity_type).update(tinysg.operations.safe_delete(field_name))

    def schema_field_read(self, entity_type: str, field_name: str) -> dict:
        """Return the schema for the given entity field.

        Args:
            entity_type (str): Entity type the field is on.
            field_name (str): Name of the field to return the schema for.

        Raises:
            tinysg.exceptions.SchemaError: If the given entity schema does not exist.
            tinysg.exceptions.SchemaError: If the given field does not exists.

        Returns:
            dict
        """

        self.schema_entity_read(entity_type)

        if field_name == "id":
            return {"name": field_name, "type": "number"}

        if field_name == "type":
            return {"name": field_name, "type": "text"}

        result = self._db.table("_fields").get(
            (where("entity_type") == entity_type) & (where("name") == field_name)
        )

        if result is None:
            raise SchemaError(f"Entity '{entity_type} has no '{field_name}' field.")
        else:
            return dict(result, id=result.doc_id)

    def schema_field_read_all(self, entity_type: str) -> List[dict]:
        """Return the schema for the given entity's fields.

        Args:
            entity_type (str): Entity type the field is on.

        Raises:
            tinysg.exceptions.SchemaError: If the given entity schema does not exist.

        Returns:
            list[dict]
        """

        self.schema_entity_read(entity_type)

        results = self._db.table("_fields").search(where("entity_type") == entity_type)
        results = [dict(result, id=result.doc_id) for result in results]

        # TODO: Add id/type fields in middleware
        results += [
            {"entity_type": entity_type, "name": "id", "type": "number"},
            {"entity_type": entity_type, "name": "type", "type": "text"},
        ]

        return results

    def schema_field_update(self, entity_type: str, field_name: str, properties: dict) -> dict:
        """Update the given field in the schema.

        Args:
            entity_type (str): Name of the entity type to update the field on.
            field_name (str): Name of the field to update.
            properties (dict): Field properties to update.

        Raises:
            tinysg.exceptions.SchemaError: If the given entity schema does not exist.
            tinysg.exceptions.SchemaError: If the given field does not exists.

        Returns:
            dict
        """

        field = self.schema_field_read(entity_type, field_name)

        old = dict(field)
        old.pop("id")

        new = dict(old)
        new.update(properties)

        tinysg.fields.validate_spec(field_name, new)

        self._db.table("_fields").update(
            tinysg.operations.replace(new),
            doc_ids=[field["id"]],
        )

        return self.schema_field_read(entity_type, field_name)
