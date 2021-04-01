from xmltotabular import XmlCollectionToTabular
from xmltotabular.sqlite_db import SqliteDB


def normalize_schema(schema):
    return " ".join(schema.split())


def test_simple_table_creation(simple_config, debug_logger):
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = SqliteDB(":memory:")

    for tablename, fieldnames in get_fieldnames(simple_config).items():
        db[tablename].create({fieldname: str for fieldname in fieldnames})

    expected_schema = """
    CREATE TABLE [album] (
        [name] TEXT,
        [artist] TEXT,
        [released] TEXT,
        [label] TEXT,
        [genre] TEXT)
    """

    assert "album" in db.table_names()
    assert normalize_schema(db["album"].schema) == normalize_schema(expected_schema)


def test_table_creation_with_fields_reversed(simple_config, debug_logger):
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = SqliteDB(":memory:")

    for tablename, fieldnames in get_fieldnames(simple_config).items():
        params = {"column_order": list(reversed(fieldnames))}
        db[tablename].create({fieldname: str for fieldname in fieldnames}, **params)

    expected_schema = """
    CREATE TABLE [album] (
        [genre] TEXT,
        [label] TEXT,
        [released] TEXT,
        [artist] TEXT,
        [name] TEXT)
    """

    assert "album" in db.table_names()
    assert normalize_schema(db["album"].schema) == normalize_schema(expected_schema)


def test_table_creation_with_pk(simple_config, debug_logger):
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = SqliteDB(":memory:")

    for tablename, fieldnames in get_fieldnames(simple_config).items():
        params = {"pk": "name"}
        db[tablename].create({fieldname: str for fieldname in fieldnames}, **params)

    expected_schema = """
    CREATE TABLE [album] (
        [name] TEXT PRIMARY KEY,
        [artist] TEXT,
        [released] TEXT,
        [label] TEXT,
        [genre] TEXT)
    """

    assert "album" in db.table_names()
    assert normalize_schema(db["album"].schema) == normalize_schema(expected_schema)


def test_table_creation_additional_fields(simple_config, debug_logger):
    get_fieldnames = XmlCollectionToTabular.get_fieldnames
    db = SqliteDB(":memory:")

    for tablename, fieldnames in get_fieldnames(simple_config).items():
        db[tablename].create({fieldname: str for fieldname in fieldnames})

    additional_fields = {"album": ["description", "notes"]}
    for tablename, fieldnames in additional_fields.items():
        if tablename in db.table_names():
            for fieldname in fieldnames:
                if fieldname not in db[tablename].column_names:
                    db[tablename].add_column(fieldname, str)

    print(f"{db['album'].schema=}")

    expected_schema = """
    CREATE TABLE [album] (
        [name] TEXT,
        [artist] TEXT,
        [released] TEXT,
        [label] TEXT,
        [genre] TEXT,
        [description] TEXT,
        [notes] TEXT)
    """

    assert "album" in db.table_names()
    assert normalize_schema(db["album"].schema) == normalize_schema(expected_schema)
