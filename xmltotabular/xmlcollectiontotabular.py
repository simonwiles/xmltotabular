import csv
import logging
import sys
from collections import defaultdict
from pathlib import Path
from pprint import pformat

import yaml

from .sqlite_db import SqliteDB
from .utils import expand_paths, colored, Pool, cpu_count, yield_xml_doc
from .xmldoctotabular import XmlDocToTabular


class XmlCollectionToTabular:
    def __init__(
        self,
        xml_input,
        config,
        output_path,
        output_type="sqlite",
        dtd_path=None,
        preprocess_doc=None,
        log_level=logging.INFO,
        **kwargs,
    ):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))

        self.xml_files = []
        if isinstance(xml_input, str):
            xml_input = [xml_input]
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

        self.config = config
        if isinstance(config, str) and Path(config).is_file():
            self.config = yaml.safe_load(open(config))

        self.output_type = output_type
        self.output_path = Path(output_path).resolve()

        if self.output_type == "sqlite":
            if self.output_path.is_dir():
                self.output_path = (self.output_path / "db.sqlite").resolve()

            if self.output_path.suffix != "sqlite":
                self.output_path = self.output_path.with_suffix(".sqlite")

            if self.output_path.exists():
                self.logger.warning(
                    colored(
                        "Sqlite database %s exists; records will be appended.",
                        "yellow",
                    ),
                    self.output_path,
                )
            else:
                self.output_path.parent.mkdir(parents=True, exist_ok=True)

            self.init_sqlite_db(self.output_path)
        else:
            self.output_path.mkdir(parents=True, exist_ok=True)

        self.dtd_path = dtd_path
        self.preprocess_doc = preprocess_doc
        self.validate = kwargs["validate"]
        self.processes = kwargs["processes"]
        self.continue_on_error = kwargs["continue_on_error"]

        self.fieldnames = self.get_fieldnames(self.config)
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

    def init_sqlite_db(self, output_path):
        self.db = SqliteDB(output_path)

        self.db.execute("pragma synchronous=off;")
        self.db.execute("pragma journal_mode=memory;")

        for tablename, fieldnames in self.get_fieldnames(self.config).items():
            if tablename in self.db.table_names():
                for fieldname in fieldnames:
                    if fieldname not in self.db[tablename].columns:
                        self.db[tablename].add_column(fieldname, str)
                continue
            params = {"column_order": fieldnames}
            if "id" in fieldnames:
                params["pk"] = "id"
            self.db[tablename].create(
                {fieldname: str for fieldname in fieldnames}, **params
            )

    def convert(self):
        if not self.xml_files:
            self.logger.warning(colored("No input files to process!", "red"))

        docParser = XmlDocToTabular(
            logger=self.logger,
            config=self.config,
            dtd_path=self.dtd_path,
            preprocess_doc=self.preprocess_doc,
            validate=self.validate,
            continue_on_error=self.continue_on_error,
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
                    docParser.process_doc_from_pool,
                    yield_xml_doc(input_file),
                    chunksize,
                )
            ):

                if i % 100 == 0:
                    self.logger.info(colored("Processing document %d...", "cyan"), i + 1)
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

        if self.output_type == "sqlite" and self.output_path == ":memory:":
            return self.db

    def flush_to_disk(self, tables):
        if self.output_type == "csv":
            self.write_csv_files(tables)

        if self.output_type == "sqlite":
            self.write_sqlitedb(tables)

    @staticmethod
    def get_fieldnames(full_config):
        # On python >=3.7, dictionaries maintain key order, so fields are guaranteed to
        #  be returned in the order in which they appear in the config file.  To
        #  guarantee this on versions of python <3.7 (insofar as it matters),
        #  collections.OrderedDict would have to be used here.

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

        for key, config in full_config.items():
            if key.startswith("<"):
                # skip keyword instructions
                continue
            add_fieldnames(config, [])

        return fieldnames

    def write_csv_files(self, tables):

        self.logger.info(
            "%s",
            colored(f"Writing csv files to {self.output_path.resolve()} ...", "green"),
        )
        for tablename, rows in tables.items():
            output_file = self.output_path / f"{tablename}.csv"

            if output_file.exists():
                self.logger.debug(
                    "%s",
                    colored(
                        f"CSV file {output_file} exists; records will be appended.",
                        "yellow",
                    ),
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
        self.logger.info(colored("Writing records to %s ...", "green"), self.output_path)
        self.db.conn.execute("begin exclusive;")
        for tablename, rows in tables.items():
            self.logger.info(
                colored("Writing %d records to `%s`...", "magenta"),
                len(rows),
                tablename,
            )
            self.db[tablename].insert_all(rows)
