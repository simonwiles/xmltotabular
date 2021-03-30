import sqlite3
from pathlib import Path

from .utils import colored


class SqliteDB:
    def __init__(self, path):
        if path == ":memory:":
            self.conn = sqlite3.connect(":memory:")
            self.path = ":memory:"
        else:
            self.path = Path(path).resolve()

            if self.path.is_dir():
                self.path = (self.path / "db.sqlite").resolve()

            if self.path.suffix != "sqlite":
                self.path = self.path.with_suffix(".sqlite")

            if self.path.exists():
                self.logger.warning(
                    colored(
                        "Sqlite database %s exists; records will be appended.",
                        "yellow",
                    ),
                    self.path,
                )
            else:
                self.path.parent.mkdir(parents=True, exist_ok=True)

            self.conn = sqlite3.connect(str(self.path), isolation_level=None)
