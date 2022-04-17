import pytest
import yaml

from xmltotabular import XmlDocToTabular
from xmltotabular.utils import WrongDoctypeException, NoDoctypeException


def test_simple_doctype_checking():

    config = yaml.safe_load(
        r"""
        <root_element>: album
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
<!DOCTYPE album SYSTEM "album.dtd" [ ]>
<album>
  <name>Five Leaves Left</name>
  <artist>Nick Drake</artist>
  <released>1969</released>
  <label>Island</label>
  <genre>Folk</genre>
</album>
    """

    docTransformer = XmlDocToTabular(config, check_doctype=True)

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


def test_doctype_checking_when_not_record_base():
    """If the root element is a container for records, not a record itself..."""

    config = yaml.safe_load(
        r"""
        <root_element>: albums
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
<!DOCTYPE albums SYSTEM "albums.dtd" [ ]>
<albums>
  <album>
    <name>Five Leaves Left</name>
    <artist>Nick Drake</artist>
    <released>1969</released>
    <label>Island</label>
    <genre>Folk</genre>
  </album>
</albums>
    """

    docTransformer = XmlDocToTabular(config, check_doctype=True)

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


def test_wrong_doctype():

    config = yaml.safe_load(
        r"""
        <root_element>: albums
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
<!DOCTYPE wrong_elem SYSTEM "albums.dtd" [ ]>
<albums>
  <album>
    <name>Five Leaves Left</name>
    <artist>Nick Drake</artist>
    <released>1969</released>
    <label>Island</label>
    <genre>Folk</genre>
  </album>
</albums>
    """

    docTransformer = XmlDocToTabular(config, check_doctype=True)
    with pytest.raises(WrongDoctypeException):
        docTransformer.process_doc(xml)


def test_no_doctype():

    config = yaml.safe_load(
        r"""
        <root_element>: albums
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
<albums>
  <album>
    <name>Five Leaves Left</name>
    <artist>Nick Drake</artist>
    <released>1969</released>
    <label>Island</label>
    <genre>Folk</genre>
  </album>
</albums>
    """

    docTransformer = XmlDocToTabular(config, check_doctype=True)
    with pytest.raises(NoDoctypeException):
        docTransformer.process_doc(xml)


def test_wrong_doctype_ignored_when_continue_on_error():

    config = yaml.safe_load(
        r"""
        <root_element>: albums
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
<!DOCTYPE wrong_elem SYSTEM "albums.dtd" [ ]>
<albums>
  <album>
    <name>Five Leaves Left</name>
    <artist>Nick Drake</artist>
    <released>1969</released>
    <label>Island</label>
    <genre>Folk</genre>
  </album>
</albums>
    """

    docTransformer = XmlDocToTabular(config, check_doctype=True, continue_on_error=True)
    assert docTransformer.process_doc(xml) == {}


def test_no_doctype_not_raised_when_continue_on_error():

    config = yaml.safe_load(
        r"""
        <root_element>: albums
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
<albums>
  <album>
    <name>Five Leaves Left</name>
    <artist>Nick Drake</artist>
    <released>1969</released>
    <label>Island</label>
    <genre>Folk</genre>
  </album>
</albums>
    """

    docTransformer = XmlDocToTabular(config, check_doctype=True, continue_on_error=True)
    assert docTransformer.process_doc(xml) == {}
