import itertools
import sqlite3
from collections import namedtuple

# > SQLITE_MAX_VARIABLE_NUMBER ... defaults to 999 for SQLite versions prior to 3.32.0
# > (2020-05-22) or 32766 for SQLite versions after 3.32.0.
#
# but many distributions ship SQLite compile with this value set much higher
#  (Debian-based Linux distros and Homebrew, for example, ship SQLite compiled with
#   SQLITE_MAX_VARIABLE_NUMBER set to 250,000).
#
# The default here is to assume the conservative value, but insert performance can be
#  increased significantly if a higher value can be used, so SqliteDB() accepts a user-
#  supplied value.  In modern versions of SQLite, the value set at compile-time can be
#  check with `echo "" | sqlite3 -cmd ".limits variable_number"`.
#
# See: https://www.sqlite.org/limits.html#max_variable_number
SQLITE_MAX_VARIABLE_NUMBER = 999

# See: https://www.sqlite.org/limits.html#max_column
SQLITE_MAX_COLUMN = 2000


COLUMN_TYPE_MAPPING = {
    float: "FLOAT",
    int: "INTEGER",
    bool: "INTEGER",
    str: "TEXT",
    bytes: "BLOB",
    # SQLite explicit types
    "TEXT": "TEXT",
    "INTEGER": "INTEGER",
    "FLOAT": "FLOAT",
    "BLOB": "BLOB",
    "text": "TEXT",
    "integer": "INTEGER",
    "float": "FLOAT",
    "blob": "BLOB",
}


class SqliteDB:
    def __init__(self, path, max_vars=None):
        self.path = path
        self.max_vars = max_vars or SQLITE_MAX_VARIABLE_NUMBER
        if path == ":memory:":
            self.conn = sqlite3.connect(":memory:")
        else:
            self.conn = sqlite3.connect(str(path), isolation_level=None)
        self.conn.execute("PRAGMA synchronous = OFF;")
        self.conn.execute("PRAGMA journal_mode = MEMORY;")
        self.conn.execute("PRAGMA locking_mode = EXCLUSIVE;")

    def __getitem__(self, table_name):
        return Table(self, table_name)

    def __repr__(self):
        return "<Database: {}>".format(self.path)

    def table_names(self):
        sql = "SELECT name FROM sqlite_master WHERE type = 'table';"
        return [r[0] for r in self.execute(sql).fetchall()]

    def execute(self, sql, parameters=None):
        if parameters is not None:
            return self.conn.execute(sql, parameters)
        else:
            return self.conn.execute(sql)

    def create_table_sql(self, name, columns, pk=None, column_order=None):
        validate_column_names(columns.keys())
        column_items = list(columns.items())
        if column_order is not None:
            column_items.sort(
                key=lambda p: column_order.index(p[0])
                if p[0] in column_order
                else SQLITE_MAX_COLUMN
            )

        column_defs = []
        if isinstance(pk, str):
            assert (
                pk in columns
            ), f"Specified primary-key ({pk}) is not in supplied columns!"
        for column_name, column_type in column_items:
            column_extras = []
            if column_name == pk:
                column_extras.append("PRIMARY KEY")
            column_defs.append(
                "   [{column_name}] {column_type}{column_extras}".format(
                    column_name=column_name,
                    column_type=COLUMN_TYPE_MAPPING[column_type],
                    column_extras=(" " + " ".join(column_extras))
                    if column_extras
                    else "",
                )
            )
        columns_sql = ",\n".join(column_defs)
        sql = f"CREATE TABLE [{name}] (\n{columns_sql}\n);"
        return sql

    def create_table(self, name, columns, pk=None, column_order=None):
        sql = self.create_table_sql(
            name=name,
            columns=columns,
            pk=pk,
            column_order=column_order,
        )
        self.execute(sql)
        return Table(self, name)


class Table:
    def __init__(self, db, name):
        self.db = db
        self.name = name

    @property
    def columns(self):
        rows = self.db.execute("PRAGMA table_info([{}])".format(self.name)).fetchall()
        return {row[1]: Column(*row) for row in rows}

    @property
    def schema(self):
        return self.db.execute(
            "SELECT sql FROM sqlite_master WHERE name = ?", (self.name,)
        ).fetchone()[0]

    def create(
        self,
        columns,
        pk=None,
        column_order=None,
    ):
        assert len(columns) <= min(self.db.max_vars, SQLITE_MAX_COLUMN), (
            f"Tables can have a maximum of {min(self.db.max_vars, SQLITE_MAX_COLUMN)} "
            "columns on this system."
        )

        with self.db.conn:
            self.db.create_table(
                self.name,
                columns,
                pk=pk,
                column_order=column_order,
            )
        return self

    def add_column(self, col_name, col_type=None):
        sql = "ALTER TABLE [{table}] ADD COLUMN [{col_name}] {col_type};".format(
            table=self.name, col_name=col_name, col_type=COLUMN_TYPE_MAPPING[col_type]
        )
        self.db.execute(sql)
        return self

    def generate_insert_batches(self, records):
        def batches(records, max_batch_size):
            """Yield successive batches of `records` of size `max_batch_size`."""
            for i in range(0, len(records), max_batch_size):
                yield records[i : i + max_batch_size]

        num_columns = len(self.columns)

        max_batch_size = self.db.max_vars // num_columns
        columns = ", ".join(f"[{c}]" for c in self.columns)
        placeholders = ", ".join("?" * num_columns)

        for batch in batches(records, max_batch_size):
            params = [
                [record.get(key, None) for key in self.columns] for record in batch
            ]
            rows = ", ".join(f"({placeholders})" for record in batch)
            sql = f"INSERT INTO [{self.name}] ({columns}) VALUES {rows};"
            yield (sql, list(itertools.chain(*params)))

    def insert_all(self, records):
        for sql, params in self.generate_insert_batches(records):
            self.db.execute(sql, params)
        self.db.conn.commit()


Column = namedtuple(
    "Column", ("cid", "name", "type", "notnull", "default_value", "is_pk")
)


def validate_column_names(column_names):
    # Columns may not contain '[' or ']' (https://bugs.python.org/issue39652)
    for column_name in column_names:
        assert (
            "[" not in column_name and "]" not in column_name
        ), "'[' and ']' cannot be used in column names"
