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
    assert sys.stdout.isatty()
    from termcolor import colored
except (AssertionError, ImportError):

    def colored(text, *args, **kwargs):
        """ Dummy function in case termcolor is not available. """
        return text


def expand_paths(path_expr):

    path = Path(path_expr).expanduser()
    if path.is_file():
        return [path]
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


class WrongDoctypeException(Exception):
    """Used to indicate that an XML doc has the wrong DOCTYPE."""


class NoDoctypeException(Exception):
    """Used to indicate that an XML doc does not specify a DOCTYPE."""


def test_doctype(doc, expected_doctype):
    for line in doc.split("\n"):
        if line.startswith(f"<!DOCTYPE {expected_doctype}"):
            return
        elif line.startswith("<!DOCTYPE "):
            raise WrongDoctypeException(line)

    raise NoDoctypeException()


def yield_xml_doc(filepath):
    filename = filepath.resolve().name
    xml_doc = []

    with open(filepath, "r", errors="replace") as _fh:
        for i, line in enumerate(_fh):
            if xml_doc and line.startswith("<?xml "):
                yield {
                    "filename": filename,
                    "linenum": i - len(xml_doc),
                    "doc": "".join(xml_doc),
                }
                xml_doc = []

            # handle the case where documents have been concatenated without
            #  adequate interpolation of new-lines
            elif xml_doc and "<?xml " in line:
                xml_doc.append(line[: line.find("<?xml ")])
                line = line[line.find("<?xml ") :]
                yield {
                    "filename": filename,
                    "linenum": i - len(xml_doc),
                    "doc": "".join(xml_doc),
                }
                xml_doc = []

            xml_doc.append(line)

        yield {
            "filename": filename,
            "linenum": i - len(xml_doc),
            "doc": "".join(xml_doc),
        }
