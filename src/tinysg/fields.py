"""Entity field functions."""

import datetime
import collections
import enum
import json

import tinysg.utils

from tinysg.filters import FilterOperator as fop
from tinysg.exceptions import FilterSpecError

DeepField = collections.namedtuple("DeepField", "head,entity_type,tail")

DEEP_FIELD_SEP = "."
DATE_FORMAT = "%Y-%m-%d"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class FieldType(enum.Enum):
    """Entity field type enums."""

    BOOL = "bool"
    DATE = "date"
    DATE_TIME = "date_time"
    ENTITY = "entity"
    ENUM = "enum"
    FLOAT = "float"
    JSON = "json"
    MULTI_ENTITY = "multi_entity"
    NUMBER = "number"
    TEXT = "text"
    TEXT_LIST = "textList"


class UpdateMode(enum.Enum):
    """Multi-entity link field update modes."""

    ADD = "add"
    REMOVE = "remove"
    SET = "set"

    @classmethod
    def values(cls):
        """Return the enum values."""

        return [
            cls.ADD.value,
            cls.REMOVE.value,
            cls.SET.value,
        ]


FIELD_OPERATORS = {
    FieldType.BOOL.value: [
        fop.IS.value,
    ],
    FieldType.DATE.value: [
        fop.BETWEEN.value,
        fop.GREATER_THAN.value,
        fop.LESS_THAN.value,
        fop.IN_CALENDAR.value,
        fop.IN.value,
        fop.IS.value,
        fop.IN_LAST.value,
        fop.IN_NEXT.value,
    ],
    FieldType.DATE_TIME.value: [
        fop.BETWEEN.value,
        fop.GREATER_THAN.value,
        fop.LESS_THAN.value,
        fop.IN_CALENDAR.value,
        fop.IN.value,
        fop.IS.value,
        fop.IN_LAST.value,
        fop.IN_NEXT.value,
    ],
    FieldType.ENTITY.value: [
        fop.IN.value,
        fop.IS.value,
        fop.TYPE_IS.value,
    ],
    FieldType.FLOAT.value: [
        fop.BETWEEN.value,
        fop.GREATER_THAN.value,
        fop.IN.value,
        fop.IS.value,
        fop.LESS_THAN.value,
    ],
    FieldType.MULTI_ENTITY.value: [
        fop.IN.value,
        fop.IS.value,
        fop.TYPE_IS.value,
    ],
    FieldType.NUMBER.value: [
        fop.BETWEEN.value,
        fop.GREATER_THAN.value,
        fop.IN.value,
        fop.IS.value,
        fop.LESS_THAN.value,
    ],
    FieldType.TEXT.value: [
        fop.CONTAINS.value,
        fop.ENDS_WITH.value,
        fop.IS.value,
        fop.IN.value,
        fop.STARTS_WITH.value,
    ],
    FieldType.TEXT_LIST.value: [
        fop.CONTAINS.value,
        fop.ENDS_WITH.value,
        fop.IS.value,
        fop.IN.value,
        fop.STARTS_WITH.value,
    ],
}


def parse_deep_field(return_field: str) -> DeepField:
    """Parse the given deep return field."""

    try:
        head, entity_type, tail = return_field.split(DEEP_FIELD_SEP, 2)
    except ValueError:
        raise FilterSpecError("Deep field must have at least three values.")

    return DeepField(head, entity_type, tail)


def is_deep_field(return_field: str) -> bool:
    """Return true if the given return field is on a linked entity."""

    return DEEP_FIELD_SEP in return_field


def is_entity(field_schema: dict) -> bool:
    """Return true if the given field is an entity field."""

    return field_schema["type"] == FieldType.ENTITY.value


def is_link(field_schema: dict) -> bool:
    """Return True if the given field is an entity or multi-entity field."""

    return is_entity(field_schema) or is_multi_entity(field_schema)


def is_multi_entity(field_schema: dict) -> bool:
    """Return true if the given field is an entity field."""

    return field_schema["type"] == FieldType.MULTI_ENTITY.value


def _field_type(field_spec: dict) -> str:
    """Return the type of the given field.

    Args:
        field_spec (dict): Entity field spec.
    Raises:
        ValueError: If the type is not given.
        ValueError: If the type is not valid.

    Returns:
        str
    """

    data_types = ", ".join([each.value for each in FieldType])

    try:
        data_type = field_spec["type"]
    except KeyError:
        raise ValueError(f"Field properties must include 'type' - {data_types}.")
    else:
        if data_type not in data_types:
            raise ValueError(f"Invalid data type '{data_type}' - expected {data_types}.")

    return data_type


def conform_spec(field_spec):
    """Conform the given field spec.

    Args:
        field_spec (dict): Entity field spec.
    """

    data_type = _field_type(field_spec)

    try:
        _CONFORM_FUNC[data_type](field_spec)
    except KeyError:
        pass


def _conform_entity(properties):
    """Conform the field properties for the given entity field.

    Args:
        properties (dict): Field spec.
    """

    if isinstance(properties["link"], str):
        properties["link"] = [properties["link"]]


def handle_value(value, field_spec):
    """Handle the value for the given field.

    Args:
        value (Any): Field value.
        field_spec (dict): Field spec.

    Returns:
        Any
    """

    value, is_valid = _HANDLER_FUNC[field_spec["type"]](value, field_spec)

    if is_valid:
        return value
    else:
        value_type = type(value)

        raise ValueError(
            f"Field '{field_spec['entity_type']}.{field_spec['name']}' expects a(n) "
            f"'{field_spec['type']}' entity, got {value_type.__name__}."
        )


def _handle_bool(value, field_spec):
    """Return the given value as a bool."""

    if value is None:
        return field_spec.get("default") or False, True

    if isinstance(value, (int, bool)):
        return bool(value), True
    else:
        return value, False


def _handle_date(value, field_spec):
    """Return the given value as a date."""

    if value is None:
        if field_spec.get("default"):
            value = tinysg.utils.today()
        else:
            return None, True

    if isinstance(value, datetime.date):
        return value.strftime(DATE_FORMAT), True
    else:
        raise ValueError("Must provide a date.")


def _handle_date_time(value, field_spec):
    """Return the given value as a date."""

    if value is None:
        if field_spec.get("default"):
            value = tinysg.utils.now()
        else:
            return None, True

    if isinstance(value, datetime.datetime):
        return value.strftime(DATE_TIME_FORMAT), True
    else:
        return value, False


def _handle_entity(value, field_spec):
    """Return the given value as an entity."""

    if value is None:
        return None, True

    if isinstance(value, dict):
        if "id" not in value:
            result = False
        elif "type" not in value:
            result = False
        elif value["type"] not in field_spec["link"]:
            raise ValueError(
                f"Field '{field_spec['entity_type']}.{field_spec['name']}' expects a "
                f"'{field_spec['link']}' entity, got {value['type']}."
            )
        else:
            value = {
                "id": value["id"],
                "type": value["type"],
            }
            result = True
    else:
        result = False

    return value, result


def _handle_enum(value, field_spec):
    if not value:
        return field_spec.get("default"), True

    enum_values = field_spec.get("values", [])

    if value in enum_values:
        return value, True
    else:
        enum_values_str = ", ".join(enum_values)

        raise ValueError(
            f"Enum field '{field_spec['entity_type']}.{field_spec['name']}' expects a "
            f"'{enum_values_str}', got '{value}'."
        )


def _handle_float(value, field_spec):
    if value is None:
        return field_spec.get("default"), True

    if isinstance(value, (int, float)):
        return float(value), True
    else:
        return value, False


def _handle_json(value, field_spec):
    if value is None:
        return None, True

    try:
        json.dumps(value)

        return value, True
    except TypeError as exc:
        raise ValueError(
            f"JSON field '{field_spec['entity_type']}.{field_spec['name']}' expects a valid JSON object - {exc}."
        )


def _handle_number(value, field_spec):
    if value is None:
        return field_spec.get("default"), True

    if isinstance(value, int):
        return int(value), True
    else:
        return value, False


def _handle_multi_entity(value, field_spec):
    """Return the given value as an entity list."""

    if value is None:
        return None, True

    results = []
    is_valid = True

    for each in value:
        result, is_valid = _handle_entity(each, field_spec)

        if is_valid:
            results.append(result)
        else:
            break

    return results, is_valid


def _handle_text(value, field_spec):
    if not value:
        return None, True

    if isinstance(value, str):
        return value, True
    else:
        return value, False


def _handle_text_list(value, field_spec):
    """Return True if the given value is a valid boolean."""

    if not value:
        return None, True

    results = []
    is_valid = True

    for each in value:
        result, is_valid = _handle_text(each, field_spec)

        if is_valid:
            results.append(result)
        else:
            break

    return results, is_valid


def update_multi_entity_field(
    old_values: list[dict], new_values: list[dict], update_mode: str = None
) -> list[dict]:
    """Return the new value for a multi-entity link field.

    Args:
        old_values (list[dict]): List of entities currently linked to the field.
        new_values (list[dict]): List of entities to update the field with.
        update_mode (str): Update mode for the field. Default is 'set'.

    Returns:
        list[dict]
    """

    update_mode = update_mode or UpdateMode.SET.value
    links = collections.OrderedDict()

    as_key = tinysg.utils.as_key

    for each in old_values:
        links[as_key(each)] = dict(each)

    if update_mode == UpdateMode.ADD.value:
        for each in new_values:
            links[as_key(each)] = each
    elif update_mode == UpdateMode.SET.value:
        links.clear()

        for each in new_values:
            links[as_key(each)] = each
    elif update_mode == UpdateMode.REMOVE.value:
        for each in new_values:
            links.pop(as_key(each), None)
    else:
        raise ValueError(f"Unsupported update mode: {update_mode}.")

    result = list(links.values())

    return result


def validate_spec(field_spec):
    """Validate the given field spec.

    Args:
        field_spec (dict): Entity field spec.
    """

    data_type = _field_type(field_spec)

    _VALIDATOR_FUNC[data_type](field_spec)


def _validate_bool(properties):
    """Validate the properties for an bool field."""

    if not isinstance(properties.get("default", False), bool):
        raise ValueError("Default value for a 'bool' field must be True/False")


def _validate_date(properties):
    """Validate the properties for an date field."""

    if not isinstance(properties.get("default", False), bool):
        raise ValueError("Default value for a 'date' field must be True/False")


def _validate_datetime(properties):
    """Validate the properties for an date field."""

    if not isinstance(properties.get("default", False), bool):
        raise ValueError("Default value for a 'datetime' field must be True/False")


def _validate_entity(properties):
    """Validate the properties for an entity field."""

    if not properties.get("link"):
        raise ValueError("Must specify 'link' entity type list for an entity field.")

    if "default" in properties:
        raise ValueError("An entity field cannot have a default.")


def _validate_enum(properties):
    """Validate the properties for an enum field."""

    if "values" not in properties:
        raise ValueError("Must specify 'values' list for an enum field.")

    if "default" in properties:
        default = properties["default"]
        values = properties["values"]

        if default and default not in values:
            values_str = ", ".join(values)

            raise ValueError(
                f"Enum field default '{default}' is not one of its allowed values: {values_str}"
            )


def _validate_float(properties):
    """Validate the properties for a float field."""

    if not isinstance(properties.get("default", 0.0), (int, float)):
        raise ValueError("Default value for a float field must be a float.")


def _validate_json(properties):
    """Validate the properties for a json field."""

    if "default" in properties:
        raise ValueError("A JSON field cannot have a default.")


def _validate_multi_entity(properties):
    """Validate the properties for a multi-entity field."""

    if not properties.get("link"):
        raise ValueError("Must specify 'link' entity type for a multi-entity field.")

    if "default" in properties:
        raise ValueError("A multi-entity field cannot have a default.")


def _validate_number(properties):
    """Validate the properties for a number field."""

    if not isinstance(properties.get("default", 0), int):
        raise ValueError("Must specify a valid default for a number field.")


def _validate_text(properties):
    """Validate the properties for a text field."""

    if "default" in properties:
        raise ValueError("A text field cannot have a default.")


def _validate_text_list(properties):
    """Validate the properties for a text list field."""

    if "default" in properties:
        raise ValueError("A text list field cannot have a default.")


_HANDLER_FUNC = {
    FieldType.BOOL.value: _handle_bool,
    FieldType.DATE.value: _handle_date,
    FieldType.DATE_TIME.value: _handle_date_time,
    FieldType.ENTITY.value: _handle_entity,
    FieldType.ENUM.value: _handle_enum,
    FieldType.FLOAT.value: _handle_float,
    FieldType.JSON.value: _handle_json,
    FieldType.NUMBER.value: _handle_number,
    FieldType.MULTI_ENTITY.value: _handle_multi_entity,
    FieldType.TEXT.value: _handle_text,
    FieldType.TEXT_LIST.value: _handle_text_list,
}

_CONFORM_FUNC = {
    FieldType.ENTITY.value: _conform_entity,
    FieldType.ENTITY.value: _conform_entity,
}

_VALIDATOR_FUNC = {
    FieldType.BOOL.value: _validate_bool,
    FieldType.DATE.value: _validate_date,
    FieldType.DATE_TIME.value: _validate_datetime,
    FieldType.ENTITY.value: _validate_entity,
    FieldType.ENUM.value: _validate_enum,
    FieldType.FLOAT.value: _validate_float,
    FieldType.JSON.value: _validate_json,
    FieldType.NUMBER.value: _validate_number,
    FieldType.MULTI_ENTITY.value: _validate_multi_entity,
    FieldType.TEXT.value: _validate_text,
    FieldType.TEXT_LIST.value: _validate_text_list,
}
