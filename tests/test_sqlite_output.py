import pytest
import re

from xmltotabular import XmlCollectionToTabular
from xmltotabular.sqlite_db import SQLITE_MAX_COLUMN


def normalize_schema(schema):
    """Normalize whitespace in SQL statements returned by SqliteDB[table].schema
    for convenient comparison."""
    return re.sub(r"\s(?=[,)])|(?<=\()\s", "", " ".join(schema.split()))


def test_simple_table_creation(empty_db, simple_config):
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = empty_db

    for tablename, fieldnames in get_fieldnames(simple_config).items():
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
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = empty_db

    for tablename, fieldnames in get_fieldnames(simple_config).items():
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
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = empty_db

    for tablename, fieldnames in get_fieldnames(simple_config).items():
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
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = empty_db

    for tablename, fieldnames in get_fieldnames(simple_config).items():
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


def test_simple_data_insertion(empty_db, simple_config, simple_data):
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = empty_db

    for tablename, fieldnames in get_fieldnames(simple_config).items():
        db[tablename].create({fieldname: str for fieldname in fieldnames})

    for tablename, rows in simple_data.items():
        db[tablename].insert_all(rows)

    selected = db.execute("SELECT * FROM album;").fetchall()

    assert selected == [("Five Leaves Left", "Nick Drake", "1969", "Island", "Folk")]


@pytest.mark.parametrize(
    "num_columns,should_error",
    ((100, False), (SQLITE_MAX_COLUMN, False), (SQLITE_MAX_COLUMN + 1, True)),
)
def test_error_if_too_many_columns(empty_db, num_columns, should_error):
    columns = {f"c{i}": str for i in range(num_columns)}
    if should_error:
        with pytest.raises(AssertionError):
            empty_db["too-many-columns"].create(columns)
    else:
        empty_db["too-many-columns"].create(columns)


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
