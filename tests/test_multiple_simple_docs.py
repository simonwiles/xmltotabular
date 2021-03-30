from xmltotabular import XmlCollectionToTabular


def test_simple_transform(simple_config):

    xml_path = "tests/test_xml/multiple_simple_docs.xml"

    collectionTransformer = XmlCollectionToTabular(
        xml_path,
        simple_config,
        None,
        ":memory:",
        "sqlite",
        validate=False,
        processes=1,
        continue_on_error=False,
    )

    db = collectionTransformer.convert()

    print(db)
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

    assert False  # noqa
