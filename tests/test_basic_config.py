import logging
import sqlite3
import yaml

from xmltotabular import XmlCollectionToTabular


def test_filename_field():
    """Tests that the <filename_field> syntax is properly implemented."""

    config = yaml.safe_load(
        r"""
        album:
          <entity>: album
          <filename_field>: source-file
          <fields>:
            name: name
            artist: artist
            released: released
            label: label
            genre: genre
        """
    )

    xml_path = "tests/test_xml_files/one_doc_per_file/*.xml"

    collectionTransformer = XmlCollectionToTabular(
        xml_path,
        config,
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
            "source-file": "drake_001.xml",
        },
        {
            "artist": "Nick Drake",
            "genre": "Folk",
            "label": "Island",
            "name": "Bryter Layter",
            "released": "1971",
            "source-file": "drake_002.xml",
        },
        {
            "artist": "Nick Drake",
            "genre": "Folk",
            "label": "Island",
            "name": "Pink Moon",
            "released": "1972",
            "source-file": "drake_003.xml",
        },
    ]
