import sys
from pathlib import Path

from lxml import etree

assert sys.version_info >= (3, 6), "Error: Python 3.6 or newer is required."

if sys.version_info < (3, 7):
    try:
        from multiprocess import Pool, cpu_count
    except ImportError:
        sys.exit(
            "Error: If running with Python < 3.7, the multiprocess library is required "
            "(e.g. pip install multiprocess)."
        )
else:
    from multiprocessing import Pool, cpu_count  # noqa: F401


try:
    from termcolor import colored
except ImportError:

    def colored(text, _color):
        """ Dummy function in case termcolor is not available. """
        return text


def expand_paths(path_expr):

    path = Path(path_expr).expanduser()
    return Path(path.root).glob(
        str(Path("").joinpath(*path.parts[1:] if path.is_absolute() else path.parts))
    )


class DTDResolver(etree.Resolver):
    def __init__(self, dtd_path):
        self.dtd_path = Path(dtd_path)

    def resolve(self, system_url, _public_id, context):
        if system_url.startswith(str(self.dtd_path)):
            return self.resolve_filename(system_url, context)
        else:
            return self.resolve_filename(
                str((self.dtd_path / system_url).resolve()), context
            )
