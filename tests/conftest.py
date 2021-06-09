import logging

import pytest
import yaml

from xmltotabular.sqlite_db import SqliteDB


@pytest.fixture
def empty_db():
    return SqliteDB(":memory:")


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


@pytest.fixture
def simple_data():
    return {
        "album": [
            {
                "name": "Five Leaves Left",
                "artist": "Nick Drake",
                "released": "1969",
                "label": "Island",
                "genre": "Folk",
            }
        ]
    }


@pytest.fixture
def debug_logger():
    logger = logging.getLogger("test")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    return logger
