import pytest
import re
import tempfile
from pathlib import Path

from xmltotabular.sqlite_db import (
    SqliteDB,
    SQLITE_MAX_VARIABLE_NUMBER,
    SQLITE_MAX_COLUMN,
)
from xmltotabular.utils import get_fieldnames_from_config


PARSED_DATA = {
    "album": [
        {
            "id": "None_0",
            "name": "Five Leaves Left",
            "artist": "Nick Drake",
            "released": "1969",
            "label": "Island",
            "genre": "Folk",
        },
        {
            "id": "None_1",
            "name": "Bryter Layter",
            "artist": "Nick Drake",
            "released": "1971",
            "label": "Island",
            "genre": "Folk",
        },
        {
            "id": "None_2",
            "name": "Pink Moon",
            "artist": "Nick Drake",
            "released": "1972",
            "label": "Island",
            "genre": "Folk",
        },
    ]
}


def normalize_schema(schema):
    """Normalize whitespace in SQL statements returned by SqliteDB[table].schema
    for convenient comparison."""
    return re.sub(r"\s(?=[,)])|(?<=\()\s", "", " ".join(schema.split()))


def test_sqlitedb_repr(empty_db):
    assert repr(empty_db) == "<Database: :memory:>"


def test_simple_table_creation(empty_db, simple_config):
    db = empty_db

    for tablename, fieldnames in get_fieldnames_from_config(simple_config).items():
        db[tablename].create({fieldname: str for fieldname in fieldnames})

    expected_schema = """
    CREATE TABLE [album] (
        [name] TEXT,
        [artist] TEXT,
        [released] TEXT,
        [label] TEXT,
        [genre] TEXT
    )
    """

    assert "album" in db.table_names()
    assert normalize_schema(db["album"].schema) == normalize_schema(expected_schema)


def test_table_creation_with_fields_reversed(empty_db, simple_config):
    db = empty_db

    for tablename, fieldnames in get_fieldnames_from_config(simple_config).items():
        params = {"column_order": list(reversed(fieldnames))}
        db[tablename].create({fieldname: str for fieldname in fieldnames}, **params)

    expected_schema = """
    CREATE TABLE [album] (
        [genre] TEXT,
        [label] TEXT,
        [released] TEXT,
        [artist] TEXT,
        [name] TEXT
    )
    """

    assert "album" in db.table_names()
    assert normalize_schema(db["album"].schema) == normalize_schema(expected_schema)


def test_table_creation_with_pk(empty_db, simple_config):
    db = empty_db

    for tablename, fieldnames in get_fieldnames_from_config(simple_config).items():
        params = {"pk": "name"}
        db[tablename].create({fieldname: str for fieldname in fieldnames}, **params)

    expected_schema = """
    CREATE TABLE [album] (
        [name] TEXT PRIMARY KEY,
        [artist] TEXT,
        [released] TEXT,
        [label] TEXT,
        [genre] TEXT
    )
    """

    assert "album" in db.table_names()
    assert normalize_schema(db["album"].schema) == normalize_schema(expected_schema)


def test_table_creation_additional_fields(empty_db, simple_config):
    db = empty_db

    for tablename, fieldnames in get_fieldnames_from_config(simple_config).items():
        db[tablename].create({fieldname: str for fieldname in fieldnames})

    additional_fields = {"album": ["description", "notes"]}
    for tablename, fieldnames in additional_fields.items():
        if tablename in db.table_names():
            for fieldname in fieldnames:
                if fieldname not in db[tablename].columns:
                    db[tablename].add_column(fieldname, str)

    expected_schema = """
    CREATE TABLE [album] (
        [name] TEXT,
        [artist] TEXT,
        [released] TEXT,
        [label] TEXT,
        [genre] TEXT,
        [description] TEXT,
        [notes] TEXT
    )
    """

    assert "album" in db.table_names()
    assert normalize_schema(db["album"].schema) == normalize_schema(expected_schema)


def test_simple_data_insertion(empty_db, simple_config):
    db = empty_db

    for tablename, fieldnames in get_fieldnames_from_config(simple_config).items():
        db[tablename].create({fieldname: str for fieldname in fieldnames})

    for tablename, rows in PARSED_DATA.items():
        db[tablename].insert_all(rows)

    selected = db.execute("SELECT * FROM album;").fetchall()

    assert selected == [
        ("Five Leaves Left", "Nick Drake", "1969", "Island", "Folk"),
        ("Bryter Layter", "Nick Drake", "1971", "Island", "Folk"),
        ("Pink Moon", "Nick Drake", "1972", "Island", "Folk"),
    ]


def test_multiple_rows_insertion(empty_db):
    db = empty_db
    tablename = "test-table"
    num_columns = 10
    num_rows = 100

    db[tablename].create({f"column_{col:02}": str for col in range(num_columns)})

    db[tablename].insert_all(
        [
            {f"column_{col:02}": f"data_{row:03}_{col:02}" for col in range(num_columns)}
            for row in range(num_rows)
        ]
    )

    selected = db.execute(f"SELECT * FROM '{tablename}';").fetchall()

    assert selected == [
        tuple(f"data_{row:03}_{col:02}" for col in range(num_columns))
        for row in range(num_rows)
    ]


def test_writing_to_filesystem(simple_config):
    dirpath = tempfile.mkdtemp()
    db_path = Path(dirpath) / "test_db.sqlite"

    db = SqliteDB(db_path)

    for tablename, fieldnames in get_fieldnames_from_config(simple_config).items():
        db[tablename].create({fieldname: str for fieldname in fieldnames})

    for tablename, rows in PARSED_DATA.items():
        db[tablename].insert_all(rows)

    selected = db.execute("SELECT * FROM album;").fetchall()

    assert selected == [
        ("Five Leaves Left", "Nick Drake", "1969", "Island", "Folk"),
        ("Bryter Layter", "Nick Drake", "1971", "Island", "Folk"),
        ("Pink Moon", "Nick Drake", "1972", "Island", "Folk"),
    ]


@pytest.mark.parametrize(
    "num_columns,should_error",
    (
        (100, False),
        (min(SQLITE_MAX_VARIABLE_NUMBER, SQLITE_MAX_COLUMN), False),
        (min(SQLITE_MAX_VARIABLE_NUMBER, SQLITE_MAX_COLUMN) + 1, True),
    ),
)
def test_error_if_too_many_columns(empty_db, num_columns, should_error):
    columns = {f"c{i}": str for i in range(num_columns)}
    if should_error:
        with pytest.raises(AssertionError):
            empty_db["too-many-columns"].create(columns)
    else:
        empty_db["too-many-columns"].create(columns)


@pytest.mark.parametrize(
    "num_columns,max_vars,should_error",
    (
        (100, 250_000, False),
        (SQLITE_MAX_COLUMN, 250_000, False),
        (SQLITE_MAX_COLUMN + 1, 250_000, True),
    ),
)
def test_error_if_too_many_columns_with_custom_max_vars(
    num_columns, max_vars, should_error
):
    db = SqliteDB(":memory:", max_vars=max_vars)
    columns = {f"c{i}": str for i in range(num_columns)}
    if should_error:
        with pytest.raises(AssertionError):
            db["too-many-columns"].create(columns)
    else:
        db["too-many-columns"].create(columns)


@pytest.mark.parametrize(
    "num_rows",
    (
        # Simplest case
        1,
        # Default SQLITE_MAX_VARIABLE_NUMBER for SQLite versions < 3.32.0 (2020-05-22)
        999,
        999 + 1,
        # Default SQLITE_MAX_VARIABLE_NUMBER for SQLite versions >= 3.32.0
        32766 + 1,
        # Default SQLITE_MAX_VARIABLE_NUMBER for SQLite distributed
        #  with Debian, Ubuntu, Homebrew etc.
        250000 + 1,
    ),
)
def test_error_if_too_many_vars(empty_db, num_rows):
    empty_db["too-many-vars"].create({"c": str})
    rows = [{"c": i} for i in range(num_rows)]
    empty_db["too-many-vars"].insert_all(rows)
