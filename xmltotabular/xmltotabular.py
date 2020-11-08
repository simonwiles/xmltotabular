import csv
import re
import sqlite3
from collections import defaultdict
from functools import partial
from io import BytesIO
from pathlib import Path
from pprint import pformat

import yaml
from lxml import etree

from .utils import expand_paths, DTDResolver, colored, Pool, cpu_count


class WrongDoctypeException(Exception):
    """Raise for my specific kind of exception"""


class NoDoctypeException(Exception):
    """Raise for my specific kind of exception"""


def test_doctype(doc, expected_doctype):
    for line in doc.split("\n"):
        if line.startswith(f"<!DOCTYPE {expected_doctype}"):
            return
        elif line.startswith("<!DOCTYPE "):
            raise WrongDoctypeException(line)

    raise NoDoctypeException()


def yield_xml_doc(filepath, xml_root):
    filename = filepath.resolve().name
    xml_doc = []

    with open(filepath, "r", errors="replace") as _fh:
        for i, line in enumerate(_fh):
            if xml_doc and line.startswith("<?xml "):
                yield (filename, i - len(xml_doc), "".join(xml_doc))
                xml_doc = []

            # handle the case where documents have been concatenated without
            #  adequate interpolation of new-lines
            elif xml_doc and "<?xml " in line:
                xml_doc.append(line[: line.find("<?xml ")])
                line = line[line.find("<?xml ") :]
                yield (filename, i - len(xml_doc), "".join(xml_doc))
                xml_doc = []

            xml_doc.append(line)

        yield (filename, i - len(xml_doc), "".join(xml_doc))


class XmlDocToTabular:
    def __init__(
        self, logger, config, dtd_path, preprocess_doc, validate, continue_on_error
    ):
        self.logger = logger
        self.config = config
        self.dtd_path = dtd_path
        self.preprocess_doc = preprocess_doc
        self.validate = validate
        self.continue_on_error = continue_on_error
        self.tables = defaultdict(list)
        # lambdas can't be pickled (without dill, at least)
        self.table_pk_idx = defaultdict(partial(defaultdict, int))

    @staticmethod
    def get_text(xpath_result):
        if isinstance(xpath_result, str):
            return re.sub(r"\s+", " ", xpath_result).strip()
        return re.sub(
            r"\s+", " ", etree.tostring(xpath_result, method="text", encoding="unicode")
        ).strip()

    def get_pk(self, tree, config):
        def get_pk_component(expression):
            elems = tree.xpath(f"./{expression}")
            assert (
                len(elems) == 1
            ), f"{len(elems)} elements found for <primary_key> component {expression}"
            return self.get_text(elems[0])

        if "<primary_key>" in config and isinstance(config["<primary_key>"], str):
            return get_pk_component(config["<primary_key>"])

        if "<primary_key>" in config and isinstance(config["<primary_key>"], list):
            return "-".join(
                get_pk_component(expression) for expression in config["<primary_key>"]
            )

        return None

    def add_string(self, path, elems, record, fieldname):
        try:
            assert len(elems) == 1
        except AssertionError as exc:
            exc.msg = (
                f"Multiple elements found for {path}! "
                + "Should your config file include a joiner, or new entity "
                + "definition?"
                + "\n\n- "
                + "\n- ".join(self.get_text(el) for el in elems)
            )
            raise

        # we've only one elem, and it's a simple mapping to a fieldname
        record[fieldname] = self.get_text(elems[0])

    def process_doc(self, payload):

        filename, linenum, doc = payload

        try:
            test_doctype(doc, self.config["xml_root"])

        except WrongDoctypeException as exc:
            self.logger.debug(
                colored("Unexpected XML document at line %d in %s: ", "yellow") + "%s",
                linenum,
                filename,
                exc,
            )
            return self.tables

        except NoDoctypeException:
            self.logger.debug(
                colored("Document at line %d in %s has no DOCTYPE?\n\n", "yellow")
                + " %s",
                linenum,
                filename,
                doc,
            )
            return self.tables

        try:
            tree = self.parse_tree(doc)
            for path, config in self.config.items():
                self.process_path(tree, path, config, filename, {})

        except LookupError as exc:
            self.logger.warning(exc.args[0])
            if not self.continue_on_error:
                raise SystemExit()

        except etree.XMLSyntaxError as exc:
            self.logger.debug(doc)
            self.logger.warning(
                colored(
                    "Unable to parse XML document ending at line %d in file %s"
                    " (enable debugging -v to dump doc to console):\n\t%s",
                    "red",
                ),
                linenum,
                filename,
                exc.msg,
            )
            if not self.continue_on_error:
                raise SystemExit()

        except AssertionError as exc:
            self.logger.debug(doc)
            pk = self.get_pk(self.parse_tree(doc), next(iter(self.config.values())))
            self.logger.warning(
                colored("Record ID %s @%d: (record has not been parsed)", "red"),
                pk,
                linenum,
            )
            self.logger.warning(exc.msg)
            if not self.continue_on_error:
                raise SystemExit()

        return self.tables

    def parse_tree(self, doc):
        if self.preprocess_doc:
            doc = self.preprocess_doc(doc)

        parser_args = {
            "load_dtd": True,
            "resolve_entities": True,
            "ns_clean": True,
            "huge_tree": True,
            "collect_ids": False,
        }

        if self.validate:
            parser_args["dtd_validation"] = True

        parser = etree.XMLParser(**parser_args)
        parser.resolvers.add(DTDResolver(self.dtd_path))
        return etree.parse(BytesIO(doc.encode("utf8")), parser)

    def process_path(
        self, tree, path, config, filename, record, parent_entity=None, parent_pk=None
    ):
        try:
            elems = [tree.getroot()]
        except AttributeError:
            elems = tree.xpath("./" + path)

        self.process_field(
            elems, tree, path, config, filename, record, parent_entity, parent_pk
        )

    def process_field(
        self,
        elems,
        tree,
        path,
        config,
        filename,
        record,
        parent_entity=None,
        parent_pk=None,
    ):

        if isinstance(config, str):
            if elems:
                self.add_string(path, elems, record, config)
            return

        if "<entity>" in config:
            # config is a new entity definition (i.e. a new record on a new table/file)
            self.process_new_entity(
                tree, elems, config, filename, parent_entity, parent_pk
            )
            return

        if "<fieldname>" in config:
            # config is extra configuration for a field on this table/file
            if "<joiner>" in config:
                if elems:
                    record[config["<fieldname>"]] = config["<joiner>"].join(
                        [self.get_text(elem) for elem in elems]
                    )
                return

            if "<enum_map>" in config:
                if elems:
                    record[config["<fieldname>"]] = config["<enum_map>"].get(
                        self.get_text(elems[0])
                    )
                return

            if "<enum_type>" in config:
                if elems:
                    record[config["<fieldname>"]] = config["<enum_type>"]
                return

            # just a mapping to a fieldname string
            if len(config) == 1:
                self.add_string(path, elems, record, config["<fieldname>"])
                return

        # We may have multiple configurations for this key (XPath expression)
        if isinstance(config, list):
            for subconfig in config:
                self.process_field(
                    elems, tree, path, subconfig, filename, record, parent_entity
                )
            return

        raise LookupError(
            f'Invalid configuration for key "{parent_entity}":'
            + "\n "
            + "\n ".join(pformat(config).split("\n"))
        )

    def process_new_entity(
        self, tree, elems, config, filename, parent_entity=None, parent_pk=None
    ):
        """Process a subtree of the xml as a new entity type, creating a new record in a
        new output table/file.
        """
        entity = config["<entity>"]
        for elem in elems:
            record = {}

            pk = self.get_pk(tree, config)
            if pk:
                record["id"] = pk
            else:
                record["id"] = f"{parent_pk}_{self.table_pk_idx[entity][parent_pk]}"
                self.table_pk_idx[entity][parent_pk] += 1

            if parent_pk:
                record[f"{parent_entity}_id"] = parent_pk
            if "<filename_field>" in config:
                record[config["<filename_field>"]] = filename
            for subpath, subconfig in config["<fields>"].items():
                self.process_path(
                    elem, subpath, subconfig, filename, record, entity, pk
                )

            self.tables[entity].append(record)


class XmlCollectionToTabular:
    def __init__(
        self,
        xml_input,
        config,
        dtd_path,
        output_path,
        output_type,
        logger,
        preprocess_doc=False,
        **kwargs,
    ):

        self.logger = logger

        self.xml_files = []
        for input_path in xml_input:
            for path in expand_paths(input_path):
                if path.is_file():
                    self.xml_files.append(path)
                elif path.is_dir():
                    self.xml_files.extend(
                        path.glob(f'{"**/" if kwargs["recurse"] else ""}*.[xX][mM][lL]')
                    )
                else:
                    self.logger.fatal("specified input is invalid")
                    exit(1)

        # do this now, because we don't want to process all that data and then find
        #  the output_path is invalid... :)
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

        self.output_type = output_type

        if self.output_type == "sqlite":
            try:
                from sqlite_utils import Database as SqliteDB  # noqa

                self.db_path = (self.output_path / "db.sqlite").resolve()
                if self.db_path.exists():
                    self.logger.warning(
                        colored(
                            "Sqlite database %s exists; records will be appended.",
                            "yellow",
                        ),
                        self.db_path,
                    )

                db_conn = sqlite3.connect(str(self.db_path), isolation_level=None)
                db_conn.execute("pragma synchronous=off;")
                db_conn.execute("pragma journal_mode=memory;")
                self.db = SqliteDB(db_conn)

            except ImportError:
                logger.debug("sqlite_utils (pip3 install sqlite-utils) not available")
                raise

        self.config = yaml.safe_load(open(config))

        self.dtd_path = dtd_path
        self.preprocess_doc = preprocess_doc
        self.validate = kwargs["validate"]
        self.processes = kwargs["processes"]
        self.continue_on_error = kwargs["continue_on_error"]

        self.fieldnames = self.get_fieldnames()
        self.set_root_config()

    def set_root_config(self):
        if "xml_root" not in self.config:
            self.config["xml_root"] = next(iter(self.config.keys()))
            self.logger.warning(
                colored(
                    "<xml_root> not explicitly set in config -- assuming <%s/>",
                    "yellow",
                ),
                self.config["xml_root"],
            )

    def convert(self):
        if not self.xml_files:
            self.logger.warning(colored("No input files to process!", "red"))

        docParser = XmlDocToTabular(
            self.logger,
            self.config,
            self.dtd_path,
            self.preprocess_doc,
            self.validate,
            self.continue_on_error,
        )

        for input_file in self.xml_files:

            self.logger.warn(colored("Processing %s...", "green"), input_file.resolve())

            processes = self.processes or cpu_count() - 1 or 1
            # chunk sizes greater than 1 result in duplicate returns because the results
            #  are pooled on the XmlDocToTabular instance
            chunksize = 1

            pool = Pool(processes=processes)

            all_tables = defaultdict(list)
            for i, tables in enumerate(
                pool.imap(
                    docParser.process_doc,
                    yield_xml_doc(input_file, self.config["xml_root"]),
                    chunksize,
                )
            ):

                if i % 100 == 0:
                    self.logger.info(
                        colored("Processing document %d...", "cyan"), i + 1
                    )
                for key, value in tables.items():
                    all_tables[key].extend(value)

            pool.close()
            pool.join()

            if all_tables:
                self.logger.info(colored("...%d records processed!", "green"), i + 1)
                self.flush_to_disk(all_tables)
            else:
                self.logger.warning(
                    colored("No records found! (config file error?)", "red")
                )

    def flush_to_disk(self, tables):
        if self.output_type == "csv":
            self.write_csv_files(tables)

        if self.output_type == "sqlite":
            self.write_sqlitedb(tables)

    def get_fieldnames(self):
        """
        On python >=3.7, dictionaries maintain key order, so fields are guaranteed to
        be returned in the order in which they appear in the config file.  To guarantee
        this on versions of python <3.7 (insofar as it matters), collections.OrderedDict
        would have to be used here.
        """

        fieldnames = defaultdict(list)

        def add_fieldnames(config, _fieldnames, parent_entity=None):
            if isinstance(config, str):
                if ":" in config:
                    _fieldnames.append(config.split(":")[0])
                    return
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
                "Invalid configuration:"
                + "\n "
                + "\n ".join(pformat(config).split("\n"))
            )

        for key, config in self.config.items():
            if key.startswith("<"):
                # skip keyword instructions
                continue
            add_fieldnames(config, [])

        return fieldnames

    def write_csv_files(self, tables):

        self.logger.info(
            colored("Writing csv files to %s ...", "green"), self.output_path.resolve()
        )
        for tablename, rows in tables.items():
            output_file = self.output_path / f"{tablename}.csv"

            if output_file.exists():
                self.logger.debug(
                    colored("CSV file %s exists; records will be appended.", "yellow"),
                    output_file,
                )

                with output_file.open("a") as _fh:
                    writer = csv.DictWriter(_fh, fieldnames=self.fieldnames[tablename])
                    writer.writerows(rows)

            else:
                with output_file.open("w") as _fh:
                    writer = csv.DictWriter(_fh, fieldnames=self.fieldnames[tablename])
                    writer.writeheader()
                    writer.writerows(rows)

    def write_sqlitedb(self, tables):
        self.logger.info(colored("Writing records to %s ...", "green"), self.db_path)
        self.db.conn.execute("begin exclusive;")
        for tablename, rows in tables.items():
            params = {"column_order": self.fieldnames[tablename], "alter": True}
            if "id" in self.fieldnames[tablename]:
                params["pk"] = "id"
                params["not_null"] = {"id"}
            self.logger.info(
                colored("Writing %d records to `%s`...", "magenta"),
                len(rows),
                tablename,
            )
            self.db[tablename].insert_all(rows, **params)
