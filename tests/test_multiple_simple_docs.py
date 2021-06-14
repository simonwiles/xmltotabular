import logging

from xmltotabular import XmlCollectionToTabular


def test_simple_transform(simple_config):

    xml_path = "tests/test_xml/multiple_simple_docs.xml"

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

    selected = db.execute("SELECT * FROM album;").fetchall()

    assert selected == [
        ("Five Leaves Left", "Nick Drake", "1969", "Island", "Folk"),
        ("Bryter Layter", "Nick Drake", "1971", "Island", "Folk"),
        ("Pink Moon", "Nick Drake", "1972", "Island", "Folk"),
    ]
