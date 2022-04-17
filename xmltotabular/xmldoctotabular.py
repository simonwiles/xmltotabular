import logging
import re
from collections import defaultdict
from functools import partial
from io import BytesIO
from pprint import pformat

from lxml import etree

from .utils import (
    DTDResolver,
    colored,
    test_doctype,
    WrongDoctypeException,
    NoDoctypeException,
)


class XmlDocToTabular:
    def __init__(
        self,
        config,
        logger=None,
        dtd_path=None,
        preprocess_doc=None,
        validate=False,
        continue_on_error=False,
        check_doctype=False,
        log_level=None,
    ):
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
            self.logger.addHandler(logging.StreamHandler())

        if log_level:
            self.logger.setLevel(log_level)

        self.config = config
        self.dtd_path = dtd_path
        self.preprocess_doc = preprocess_doc
        self.validate = validate
        self.continue_on_error = continue_on_error
        self.check_doctype = check_doctype
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

    def resolve_namespaces_in_xpath(self, expression):

        if "_" in self.ns_map:
            expression = "/".join(
                f"_:{_}" if ":" not in _ and _[0] not in "@[" else _
                for _ in expression.split("/")
            )

        return expression

    def get_pk(self, tree, config):
        def get_pk_component(expression):
            expression = self.resolve_namespaces_in_xpath(expression)
            elems = tree.xpath(expression, namespaces=self.ns_map)
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

    def process_doc_from_pool(self, payload):
        """Unpack a tuple returned by yield_xml_doc().

        This is necessary because multiprocessing.Pool.imap will only pass a single
        argument to child processes. Pool.starmap would be great, but there's no version
        that works with iterables, which is really needed here. Pathos' multiprocess can
        handle this (and is required by this library when using python 3.6 anyway) --
        there may be value in using this anyway.
        """
        return self.process_doc(**payload)

    def do_doctype_check(self, doc, filename, linenum):
        try:
            test_doctype(doc, self.config["<root_element>"])
            return True

        except WrongDoctypeException as exc:
            self.logger.info(
                "%s\n%s",
                colored(
                    "Unexpected XML document"
                    + (f" ending at line {linenum}" if linenum else "")
                    + (f" in file {filename}" if filename else "")
                    + ": ",
                    "yellow",
                ),
                str(exc),
            )

            if not self.continue_on_error:
                raise

            return False

        except NoDoctypeException:
            self.logger.info(
                "%s\n",
                colored(
                    "Document"
                    + (f" ending at line {linenum}" if linenum and linenum > -1 else "")
                    + (f" in file {filename}" if filename else "")
                    + " has no DOCTYPE:",
                    "yellow",
                ),
            )

            if not self.continue_on_error:
                raise

            return False

    def process_doc(self, doc, filename=None, linenum=None):
        if self.check_doctype and not self.do_doctype_check(doc, filename, linenum):
            # doctype check failed, but continue_on_error is True
            return self.tables

        try:
            tree = self.parse_tree(doc)

            try:
                tree = tree.getroot()
            except AttributeError:
                pass

            if self.__dict__.get("ns_map", None) is None:
                self.ns_map = {
                    k if k is not None else "_": v for k, v in tree.nsmap.items()
                }
                self.ns_map_reversed = {v: k for k, v in self.ns_map.items()}

            for path, config in self.config.items():
                if path == "<root_element>":
                    continue
                self.process_path(tree, path, config, filename, {})

        except LookupError as exc:
            self.logger.warning(exc.args[0])
            if not self.continue_on_error:
                raise SystemExit() from None

        except etree.XMLSyntaxError as exc:
            self.logger.warning(
                colored(
                    "Unable to parse XML document"
                    + (f" ending at line {linenum}" if linenum else "")
                    + (f" in file {filename}" if filename else "")
                    + " (enable debug logging to dump doc to console):",
                    "red",
                )
                + colored(f"\n    {exc.msg}", "yellow")
            )
            self.logger.debug(doc)

            if not self.continue_on_error:
                raise SystemExit() from None

        except AssertionError as exc:
            pk = self.get_pk(self.parse_tree(doc), next(iter(self.config.values())))
            self.logger.warning(
                colored(
                    "Unable to parse document"
                    + (f" with ID {pk}" if pk else "")
                    + (f" ending at line {linenum}" if linenum else "")
                    + (f" in file {filename}" if filename else "")
                    + " -- record has not been parsed"
                    + " (enable debug logging to dump doc to console):",
                    "red",
                )
                + colored(f"\n    {exc.msg}", "yellow")
            )
            self.logger.debug(doc)

            if not self.continue_on_error:
                raise SystemExit() from None

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
        if self.dtd_path:
            parser.resolvers.add(DTDResolver(self.dtd_path))
        return etree.parse(BytesIO(doc.encode("utf8")), parser)

    def process_path(
        self, tree, path, config, filename, record, parent_entity=None, parent_pk=None
    ):
        path = self.resolve_namespaces_in_xpath(path)
        tag = tree.tag
        if self.ns_map:
            tag = re.sub(
                r"^{([^}]+)}",
                lambda m: f"{self.ns_map_reversed[m.group(1)]}:",
                tree.tag,
            )

        if path == tag:
            results = [tree]
        else:
            results = tree.xpath(path, namespaces=self.ns_map)
            if len(results) > 1 and not any(
                key in config
                for key in ("<entity>", "<joiner>", "<enum_map>", "<enum_type>")
            ):
                self.logger.warning(
                    f"Multiple elements found for {path}!  Only the last will be kept! "
                    + "Should your config file include a joiner, or new entity "
                    + "definition?"
                    + "\n\n- "
                    + "\n- ".join(self.get_text(el) for el in results)
                )

        for result in results:
            self.process_field(
                result, config, filename, record, parent_entity, parent_pk
            )

    def process_field(
        self,
        result,
        config,
        filename,
        record,
        parent_entity=None,
        parent_pk=None,
    ):

        if isinstance(config, str):
            record[config] = self.get_text(result)
            return

        if "<entity>" in config:
            # config is a new entity definition (i.e. a new record on a new table/file)
            self.process_new_entity(result, config, filename, parent_entity, parent_pk)
            return

        if "<fieldname>" in config:
            # config is extra configuration for a field on this table/file
            if "<joiner>" in config:
                if record.get(config["<fieldname>"]):
                    record[config["<fieldname>"]] += config["<joiner>"] + self.get_text(
                        result
                    )
                else:
                    record[config["<fieldname>"]] = self.get_text(result)
                return

            if "<enum_map>" in config:
                record[config["<fieldname>"]] = config["<enum_map>"].get(
                    self.get_text(result)
                )
                return

            if "<enum_type>" in config:
                record[config["<fieldname>"]] = config["<enum_type>"]
                return

            # just an explicit mapping to a fieldname string
            if len(config) == 1:
                record[config["<fieldname>"]] = self.get_text(result)
                return

        # We may have multiple configurations for this key (XPath expression)
        if isinstance(config, list):
            for subconfig in config:
                self.process_field(
                    result,
                    subconfig,
                    filename,
                    record,
                    parent_entity,
                    parent_pk,
                )
            return

        raise LookupError(
            f'Invalid configuration for key "{parent_entity}":'
            + "\n "
            + "\n ".join(pformat(config).split("\n"))
        )

    def process_new_entity(
        self, elem, config, filename, parent_entity=None, parent_pk=None
    ):
        """Process a subtree of the xml as a new entity type, creating a new record in a
        new output table/file.
        """
        entity = config["<entity>"]
        record = {}

        pk = self.get_pk(elem, config)
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
            self.process_path(elem, subpath, subconfig, filename, record, entity, pk)

        self.tables[entity].append(record)
