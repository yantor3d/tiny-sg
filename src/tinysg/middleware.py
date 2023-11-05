"""Database middleware."""

import collections
import frozendict
import operator

from tinydb.middlewares import CachingMiddleware, Middleware
from typing import Tuple


class ReadCachingMiddleware(CachingMiddleware):
    """Middleware that only caches reads."""

    def write(self, data: dict) -> dict:
        """Write data to the cache."""

        self._cache_modified_count = 0
        self.cache = None

        self.storage.write(data)

    def flush(self):
        """Flush data from the cache."""

        self.write(self.cache)
        self.read()


class PivotTableMiddleware(Middleware):
    """Middleware to handle the link field pivot tables.

    On read, the pivot tables are joined in the entity fields that refer to them.
    On write, the pivot tables are created from the entity fields that refer to them.

    This is done to shape the data in a way that tinydb can query, while keeping the
    data light on disk by not duplicating values for bi-directional link fields.
    """

    def read(self) -> dict:
        """Read the database.

        Returns:
            dict
        """

        data = self.storage.read()

        schema = data.get("_schema", {})
        fields = data.setdefault("_fields", {})

        entity_fields = collections.defaultdict(list)

        for field in fields.values():
            entity_type = field["entity_type"]

            entity_fields[entity_type].append(field)

        link_tables = set()

        for entity_info in schema.values():
            entity_type = entity_info["entity_type"]
            entity_info["fields"] = entity_fields[entity_type]

            table = data.get(entity_type, {})
            retired = data.get(f"Retired:{entity_type}", {})

            for entity_id, entity in table.items():
                if str(entity_id) not in retired:
                    entity["id"] = int(entity_id)
                    entity["type"] = entity_type

            for field in entity_fields[entity_type]:
                if field["type"] in [
                    "entity",
                    "multi_entity",
                ]:
                    self._join(data, entity_type, field)
                    link_tables.add(field["table"])

        for each in link_tables:
            data.pop(each, None)

        return data

    def _join(self, data: dict, entity_type: str, field: dict) -> None:
        """Join the link field pivot table with the entity data.

        Args:
            data (dict): In memory database contents.
            entity_type (str): Entity type to perform the join on.
            field (dict): Spec for the link field to join the pivot table of.
        """

        this_entity_type = entity_type
        this_entity_table = data.get(this_entity_type, {})

        link_entity_type = field["link"]
        link_entity_table = data.get(field["table"], {})

        linked_entity_list = collections.defaultdict(list)

        this_key, link_key = self._link_keys(field)

        for link in link_entity_table.values():
            this_entity_id = int(link[this_key])
            link_entity_id = int(link[link_key])

            link_entity = {"type": link_entity_type, "id": link_entity_id}

            linked_entity_list[this_entity_id].append(link_entity)

        for entity_id, entity in this_entity_table.items():
            links = linked_entity_list.get(int(entity_id))

            if links is None:
                continue

            links.sort(key=operator.itemgetter("id"))

            if field["type"] == "entity":
                entity[field["name"]] = next(iter(links), None)
            else:
                entity[field["name"]] = links

    def _link_keys(self, field: dict) -> Tuple[str, str]:
        this_key = "{entity_type}.{name}".format(**field)

        try:
            link_key = "{link}.{link_field}".format(**field)
        except KeyError:
            link_key = field["link"]

        return this_key, link_key

    def write(self, data):
        """Write the database.

        Args:
            data (dict): In memory database contents.
        """

        schema = data.get("_schema", {})

        fields = collections.defaultdict(list)

        for field in data.get("_fields", {}).values():
            entity_type = field["entity_type"]

            fields[entity_type].append(field)

        link_tables = collections.defaultdict(set)

        for entity_info in schema.values():
            entity_type = entity_info["entity_type"]

            table = data.get(entity_type, {})

            drop_field_names = ["id", "type"]
            link_fields = []

            for field in fields[entity_type]:
                if field["type"] in [
                    "entity",
                    "multi_entity",
                ]:
                    drop_field_names.append(field["name"])
                    link_fields.append(field)

            for __, entity in table.items():
                if not entity:
                    continue

                for field in link_fields:
                    this_key, link_key = self._link_keys(field)

                    if field["type"] == "entity":
                        links = [entity.get(field["name"])]
                    elif field["type"] == "multi_entity":
                        links = entity.get(field["name"]) or []

                    links = [link for link in links if link]

                    for link in links:
                        link_dict = frozendict.frozendict(
                            {link_key: link["id"], this_key: entity["id"]}
                        )

                        link_tables[field["table"]].add(link_dict)

                for field_name in drop_field_names:
                    entity.pop(field_name, None)

        for table, links in sorted(link_tables.items()):
            data[table] = {i: dict(link) for i, link in enumerate(links, 1)}

        self.storage.write(data)

        return data
