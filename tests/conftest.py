import pytest
import yaml


@pytest.fixture
def simple_config():

    return yaml.safe_load(
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
