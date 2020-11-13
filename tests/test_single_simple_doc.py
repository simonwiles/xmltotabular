import logging

from xmltotabular import XmlDocToTabular

logger = logging.getLogger("test")


def test_simple_transform(simple_config):
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

    docTransformer = XmlDocToTabular(logger, simple_config)

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
