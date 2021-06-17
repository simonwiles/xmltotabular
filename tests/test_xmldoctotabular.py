from xmltotabular import XmlDocToTabular


def test_simple_transform(simple_config, simple_data):
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

    assert docTransformer.process_doc(xml) == simple_data
