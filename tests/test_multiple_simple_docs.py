import logging

from xmltotabular import XmlCollectionToTabular

logger = logging.getLogger("test")


def test_simple_transform(simple_config):

    xml_path = "test_xml/multiple_simple_docs.xml"

    collectionTransformer = XmlCollectionToTabular(
        xml_path,
        simple_config,
        logger,
    )

    assert collectionTransformer.convert()

    expected = {  # noqa
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
