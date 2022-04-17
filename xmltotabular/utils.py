import sys
from collections import defaultdict
from pathlib import Path
from pprint import pformat

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
        """Dummy function to pass text through without escape codes if stdout is not a
        TTY or termcolor is not available."""
        return text


def expand_paths(path_expr):
    """Given a path expression, return a list of paths."""
    path = Path(path_expr).expanduser()
    if path.is_file():
        return [path]
    return Path(path.root).glob(
        str(Path("").joinpath(*path.parts[1:] if path.is_absolute() else path.parts))
    )


class DTDResolver(etree.Resolver):
    """A DTDResolver class which resolves DTDs relative to a supplied base path."""

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


def test_doctype(doc, root_element):
    """Test an XML document (passed as a string) for a DOCTYPE declaration that matches
    `root_element`."""
    for line in doc.split("\n"):
        if line.split()[:2] == ["<!DOCTYPE", root_element]:
            return True
        elif line.startswith("<!DOCTYPE "):
            raise WrongDoctypeException(line)

    raise NoDoctypeException()


def yield_xml_doc(filepath):
    """Given a path to a file containing one or more XML documents, for each document
    yield a dictionary containing a document, the filename, and the ending line number
    at which the document is found."""
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


def get_fieldnames_from_config(full_config):
    """Parse a config object and return a dictionary where keys are table names and
    values are lists of field names."""

    # On python >=3.7, dictionaries maintain key order, so fields are guaranteed to
    #  be returned in the order in which they appear in the config file.  To
    #  guarantee this on versions of python <3.7 (insofar as it matters),
    #  collections.OrderedDict would have to be used here.

    fieldnames = defaultdict(list)

    def add_fieldnames(config, _fieldnames, parent_entity=None):
        if isinstance(config, str):
            _fieldnames.append(config)
            return

        if "<fieldname>" in config:
            _fieldnames.append(config["<fieldname>"])
            return

        if "<entity>" in config:
            entity = config["<entity>"]
            _fieldnames = []
            if "<primary_key>" in config or parent_entity:
                _fieldnames.append("id")
            if parent_entity:
                _fieldnames.append(f"{parent_entity}_id")
            if "<filename_field>" in config:
                _fieldnames.append(config["<filename_field>"])
            for subconfig in config["<fields>"].values():
                add_fieldnames(subconfig, _fieldnames, entity)
            # different keys (XPath expressions) may be appending rows to the same
            #  table(s), so we're appending to lists of fieldnames here.
            fieldnames[entity] = list(
                dict.fromkeys(fieldnames[entity] + _fieldnames).keys()
            )
            return

        # We may have multiple configurations for this key (XPath expression)
        if isinstance(config, list):
            for subconfig in config:
                add_fieldnames(subconfig, _fieldnames, parent_entity)
            return

        raise LookupError(
            "Invalid configuration:" + "\n " + "\n ".join(pformat(config).split("\n"))
        )

    for key, config in full_config.items():
        if key.startswith("<"):
            # skip keyword instructions
            continue
        add_fieldnames(config, [])

    return fieldnames
