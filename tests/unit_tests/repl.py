"""Open a REPL for the connection."""

import os
import shutil
import tempfile

import tinysg

db_path = os.path.join(os.path.dirname(__file__), "db.json")

with tempfile.NamedTemporaryFile(delete=False) as tmp:
    shutil.copy(db_path, tmp.name)

sg = tinysg.Connection(tmp.name)

print(f"Use 'sg' to introspect the test database - file path: {tmp.name}.")
