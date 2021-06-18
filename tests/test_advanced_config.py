import yaml

from xmltotabular import XmlDocToTabular


def test_concatenation_of_multiple_results():
    """Tests that the <joiner> syntax is properly implemented."""

    config = yaml.safe_load(
        r"""
        album:
          <entity>: album
          <fields>:
            name: name
            artist: artist
            released: released
            label: label
            genre: genre
            description/p:
              <fieldname>: description
              <joiner>: "\n"
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
    <description>
      <p>
        Five Leaves Left was recorded between July 1968 and June 1969 at Sound Techniques
        in London, England. Engineer John Wood recalled that "[Drake] would track live,
        singing and playing along with the string section" without the use of any
        overdubbing. For the song "River Man", producer Joe Boyd described Drake playing
        on a stool in the centre of the studio while surrounded by a semi-circle of
        instruments. The studio's environment was also an important factor as it had
        multiple levels to it which enabled the creation of interesting sounds and
        atmospheres.
      </p>
      <p>
        Among his various backing musicians, Drake was accompanied by Richard Thompson
        from Fairport Convention and Danny Thompson of Pentangle. Robert Kirby, a friend
        of Drake's from his youth, arranged the string instruments for several tracks
        while Harry Robinson arranged the strings for "River Man". The title of the album
        is a reference to the old Rizla cigarette papers packet, which used to contain a
        printed note near the end saying "Only five leaves left".
      </p>
    </description>
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
                "description": (
                    "Five Leaves Left was recorded between July 1968 and June "
                    "1969 at Sound Techniques in London, England. Engineer John Wood "
                    'recalled that "[Drake] would track live, singing and playing along '
                    'with the string section" without the use of any overdubbing. For '
                    'the song "River Man", producer Joe Boyd described Drake playing on '
                    "a stool in the centre of the studio while surrounded by a semi-"
                    "circle of instruments. The studio's environment was also an "
                    "important factor as it had multiple levels to it which enabled the "
                    "creation of interesting sounds and atmospheres.\nAmong his various "
                    "backing musicians, Drake was accompanied by Richard Thompson from "
                    "Fairport Convention and Danny Thompson of Pentangle. Robert Kirby, "
                    "a friend of Drake's from his youth, arranged the string "
                    "instruments for several tracks while Harry Robinson arranged the "
                    'strings for "River Man". The title of the album is a reference to '
                    "the old Rizla cigarette papers packet, which used to contain a "
                    'printed note near the end saying "Only five leaves left".'
                ),
            }
        ]
    }
