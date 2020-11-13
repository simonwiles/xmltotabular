import pytest
import yaml


@pytest.fixture
def simple_config():
    config = """
    album:
      <entity>: album
      <fields>:
        name: name
        artist: artist
        released: released
        label: label
        genre: genre
    """
    return yaml.safe_load(config)
