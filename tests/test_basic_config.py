import logging
import sqlite3
import yaml

from xmltotabular import XmlDocToTabular, XmlCollectionToTabular


def test_primary_key():

    config = yaml.safe_load(
        r"""
        album:
          <entity>: album
          <primary_key>: name
          <fields>:
            name: name
            artist: artist
            released: released
            label: label
            genre: genre
        """
    )

    xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<album>
  <name>Five Leaves Left</name>
  <artist>Nick Drake</artist>
  <released>1969</released>
  <label>Island</label>
  <genre>Folk</genre>
</album>
    """

    docTransformer = XmlDocToTabular(config)

    assert docTransformer.process_doc(xml) == {
        "album": [
            {
                "id": "Five Leaves Left",
                "name": "Five Leaves Left",
                "artist": "Nick Drake",
                "released": "1969",
                "label": "Island",
                "genre": "Folk",
            }
        ]
    }


def test_composed_primary_key():

    config = yaml.safe_load(
        r"""
        album:
          <entity>: album
          <primary_key>:
            - name
            - artist
            - released
          <fields>:
            name: name
            artist: artist
            released: released
            label: label
            genre: genre
        """
    )

    xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<album>
  <name>Five Leaves Left</name>
  <artist>Nick Drake</artist>
  <released>1969</released>
  <label>Island</label>
  <genre>Folk</genre>
</album>
    """

    docTransformer = XmlDocToTabular(config)

    assert docTransformer.process_doc(xml) == {
        "album": [
            {
                "id": "Five Leaves Left-Nick Drake-1969",
                "name": "Five Leaves Left",
                "artist": "Nick Drake",
                "released": "1969",
                "label": "Island",
                "genre": "Folk",
            }
        ]
    }


def test_namespaced_primary_key():

    config = yaml.safe_load(
        r"""
        album:
          <entity>: album
          <primary_key>: "dc:title"
          <fields>:
            "dc:title": name
            artist: artist
            released: released
            label: label
            genre: genre
        """
    )

    xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<album xmlns:dc="http://purl.org/dc/elements/1.1/">
  <dc:title>Five Leaves Left</dc:title>
  <artist>Nick Drake</artist>
  <released>1969</released>
  <label>Island</label>
  <genre>Folk</genre>
</album>
    """

    docTransformer = XmlDocToTabular(config)

    assert docTransformer.process_doc(xml) == {
        "album": [
            {
                "id": "Five Leaves Left",
                "name": "Five Leaves Left",
                "artist": "Nick Drake",
                "released": "1969",
                "label": "Island",
                "genre": "Folk",
            }
        ]
    }


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
    )
