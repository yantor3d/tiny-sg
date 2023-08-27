"""TinyDB operations."""


def safe_delete(field):
    """Delete the given field from the document."""

    def transform(doc):
        doc.pop(field, None)

    return transform


def replace(data):
    """Replace all data in the document."""

    def transform(doc):
        doc.clear()
        doc.update(data)

    return transform
