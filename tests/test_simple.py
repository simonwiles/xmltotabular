import logging

import yaml

from xmltotabular import XmlDocToTabular

logger = logging.getLogger("test")


def test_simple_transform():
    xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <album>
      <name>Five Leaves Left</name>
      <artist>Nick Drake</artist>
      <released>1969</released>
      <label>Island</label>
      <genre>Folk</genre>
    </album>
    """.strip()

    config_yaml = """
    album:
      <entity>: album
      <fields>:
        name: name
        artist: artist
        released: released
        label: label
        genre: genre
    """

    config = yaml.safe_load(config_yaml)

    docTransformer = XmlDocToTabular(logger, config)

    assert docTransformer.process_doc(("", 0, xml)) == {
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
