import logging
import sqlite3

from xmltotabular import XmlCollectionToTabular


def test_multiple_docs_in_one_file(simple_config):

    xml_path = "tests/test_xml_files/multiple_simple_docs.xml"

    collectionTransformer = XmlCollectionToTabular(
        xml_path,
        simple_config,
        ":memory:",
        "sqlite",
        validate=False,
        processes=1,
        continue_on_error=False,
        log_level=logging.DEBUG,
    )

    db = collectionTransformer.convert()
    db.conn.row_factory = sqlite3.Row

    result = [dict(row) for row in db.execute("SELECT * FROM album;").fetchall()]

    assert result == [
        {
            "artist": "Nick Drake",
            "genre": "Folk",
            "label": "Island",
            "name": "Five Leaves Left",
            "released": "1969",
        },
        {
            "artist": "Nick Drake",
            "genre": "Folk",
            "label": "Island",
            "name": "Bryter Layter",
            "released": "1971",
        },
        {
            "artist": "Nick Drake",
            "genre": "Folk",
            "label": "Island",
            "name": "Pink Moon",
            "released": "1972",
        },
    ]


def test_multiple_docs_in_multiple_files(simple_config):

    xml_path = "tests/test_xml_files/one_doc_per_file/*.xml"

    collectionTransformer = XmlCollectionToTabular(
        xml_path,
        simple_config,
        ":memory:",
        "sqlite",
        validate=False,
        processes=1,
        continue_on_error=False,
        log_level=logging.DEBUG,
    )

    db = collectionTransformer.convert()
    db.conn.row_factory = sqlite3.Row

    result = [dict(row) for row in db.execute("SELECT * FROM album;").fetchall()]

    # Order of row insertion is not guaranteed (because of the file globbing as well
    # as because of the parallelization).
    assert all(
        record in result
        for record in [
            {
                "artist": "Nick Drake",
                "genre": "Folk",
                "label": "Island",
                "name": "Five Leaves Left",
                "released": "1969",
            },
            {
                "artist": "Nick Drake",
                "genre": "Folk",
                "label": "Island",
                "name": "Bryter Layter",
                "released": "1971",
            },
            {
                "artist": "Nick Drake",
                "genre": "Folk",
                "label": "Island",
                "name": "Pink Moon",
                "released": "1972",
            },
        ]
    )
