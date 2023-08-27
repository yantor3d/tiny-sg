"""Database exceptions."""


class DatabaseError(Exception):
    """Base class for all database errors."""


class EntityNotFound(DatabaseError):
    """Error raised when a linked entity does not exist."""


class FilterSpecError(DatabaseError):
    """Error raised when an invalid filter spec is given."""


class SchemaError(DatabaseError):
    """Error raised when an invalid entity type is specified."""
