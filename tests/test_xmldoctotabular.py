import yaml

from xmltotabular import XmlDocToTabular


def test_single_simple_entity_per_doc(simple_config):
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

    docTransformer = XmlDocToTabular(simple_config)

    assert docTransformer.process_doc(xml) == {
        "album": [
            {
                "id": "None_0",
                "name": "Five Leaves Left",
                "artist": "Nick Drake",
                "released": "1969",
                "label": "Island",
                "genre": "Folk",
            }
        ]
    }


def test_multiple_simple_entities_per_doc(simple_config):
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
  <album>
    <name>Bryter Layter</name>
    <artist>Nick Drake</artist>
    <released>1971</released>
    <label>Island</label>
    <genre>Folk</genre>
  </album>
  <album>
    <name>Pink Moon</name>
    <artist>Nick Drake</artist>
    <released>1972</released>
    <label>Island</label>
    <genre>Folk</genre>
  </album>
</albums>
    """

    docTransformer = XmlDocToTabular(simple_config)

    assert docTransformer.process_doc(xml) == {
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


def test_attribute_style_xml():

    config = yaml.safe_load(
        r"""
        album:
          <entity>: album
          <fields>:
            "@name": name
            "@artist": artist
            "@released": released
            "@label": label
            "@genre": genre
        """
    )

    xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<albums>
  <album name="Five Leaves Left"
         artist="Nick Drake"
         released="1969"
         label="Island"
         genre="Folk" />
  <album name="Bryter Layter"
         artist="Nick Drake"
         released="1971"
         label="Island"
         genre="Folk" />
  <album name="Pink Moon"
         artist="Nick Drake"
         released="1972"
         label="Island"
         genre="Folk" />
</albums>
    """

    docTransformer = XmlDocToTabular(config)

    assert docTransformer.process_doc(xml) == {
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


def test_default_namespace_resolution():

    config = yaml.safe_load(
        """
    album:
        <entity>: album
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
<album xmlns="http://example.com/albums">
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
                "id": "None_0",
                "name": "Five Leaves Left",
                "artist": "Nick Drake",
                "released": "1969",
                "label": "Island",
                "genre": "Folk",
            }
        ]
    }


def test_complex_namespace_resolution():

    config = yaml.safe_load(
        """
    album:
        <entity>: album
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
<album xmlns="http://example.com/albums" xmlns:dc="http://purl.org/dc/elements/1.1/">
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
                "id": "None_0",
                "name": "Five Leaves Left",
                "artist": "Nick Drake",
                "released": "1969",
                "label": "Island",
                "genre": "Folk",
            }
        ]
    }


def test_namespace_resolution_with_no_default():

    config = yaml.safe_load(
        """
    album:
        <entity>: album
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
                "id": "None_0",
                "name": "Five Leaves Left",
                "artist": "Nick Drake",
                "released": "1969",
                "label": "Island",
                "genre": "Folk",
            }
        ]
    }
