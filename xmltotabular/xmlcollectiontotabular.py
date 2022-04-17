import csv
import logging
import sys
from collections import defaultdict
from pathlib import Path

import yaml

from .sqlite_db import SqliteDB
from .utils import (
    expand_paths,
    colored,
    Pool,
    cpu_count,
    yield_xml_doc,
    get_fieldnames_from_config,
)
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
        recurse=True,
        validate=False,
        check_doctype=False,
        processes=None,
        continue_on_error=False,
        sqlite_max_vars=None,
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
                        path.glob(f'{"**/" if recurse else ""}*.[xX][mM][lL]')
                    )
                else:
                    self.logger.fatal("specified input is invalid")
                    exit(1)

        self.config = config
        if isinstance(config, str) and Path(config).is_file():
            self.config = yaml.safe_load(open(config))

        self.output_type = output_type
        self.output_path = output_path

        if self.output_type == "sqlite" and self.output_path == ":memory:":
            self.init_sqlite_db(self.output_path, max_vars=sqlite_max_vars)

        elif self.output_type == "sqlite":
            self.output_path = Path(self.output_path).resolve()

            if self.output_path.is_dir():
                self.output_path = (self.output_path / "db.sqlite").resolve()

            if self.output_path.suffix != "sqlite":
                self.output_path = self.output_path.with_suffix(".sqlite")

            if self.output_path.exists():
                self.logger.warning(
                    colored(
                        "Database %s exists; tables and/or rows will be appended.",
                        "yellow",
                    ),
                    self.output_path,
                )
            else:
                self.output_path.parent.mkdir(parents=True, exist_ok=True)

            self.init_sqlite_db(self.output_path, max_vars=sqlite_max_vars)

        else:
            self.output_path = Path(self.output_path).resolve()
            self.output_path.mkdir(parents=True, exist_ok=True)

        self.dtd_path = dtd_path
        self.preprocess_doc = preprocess_doc
        self.validate = validate
        self.processes = processes
        self.continue_on_error = continue_on_error
        self.check_doctype = check_doctype

        self.fieldnames = get_fieldnames_from_config(self.config)
        if check_doctype:
            self.set_root_element()

    def set_root_element(self):
        if "<root_element>" not in self.config:
            self.config["<root_element>"] = next(iter(self.config.keys()))
            self.logger.warning(
                colored(
                    "<root_element> not explicitly set in config -- assuming <%s/>",
                    "yellow",
                ),
                self.config["<root_element>"],
            )

    def init_sqlite_db(self, output_path, max_vars):
        self.db = SqliteDB(output_path, max_vars=max_vars)

        for tablename, fieldnames in get_fieldnames_from_config(self.config).items():
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
            check_doctype=self.check_doctype,
        )

        for input_file in self.xml_files:

            self.logger.warning(
                colored("Processing %s...", "green"), input_file.resolve()
            )

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

            self.logger.info(colored("...%d documents processed!", "green"), i + 1)
            if all_tables:
                self.write_tables(all_tables)
            else:
                self.logger.warning(
                    colored("No rows found! (config file error?)", "red")
                )

        if self.output_type == "sqlite" and self.output_path == ":memory:":
            return self.db

    def write_tables(self, tables):
        if self.output_type == "csv":
            self.write_csv_files(tables)

        if self.output_type == "sqlite":
            self.write_sqlitedb(tables)

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
                        f"CSV file {output_file} exists; rows will be appended.",
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
        self.logger.info(colored("Writing tables to %s ...", "green"), self.output_path)
        self.db.conn.execute("begin exclusive;")
        for tablename, rows in tables.items():
            self.logger.info(
                colored("Writing %d rows to `%s`...", "magenta"),
                len(rows),
                tablename,
            )
            self.db[tablename].insert_all(rows)
