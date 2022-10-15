import functools

from .base_lci import LCIImporter
from ..extractors.ilcd import ILCDExtractor
from ..strategies.ilcd import rename_activity_keys, get_activity_unit,convert_to_default_units
from ..strategies.generic import assign_only_product_as_production
from ..strategies.migrations import migrate_exchanges,migrate_datasets


class ILCDImporter(LCIImporter):
    def __init__(self, dirpath, dbname):

        self.dbname = dbname
        self.data = ILCDExtractor._extract(dirpath)
        self.strategies = [
            rename_activity_keys,
            get_activity_unit,
            convert_to_default_units,
            assign_only_product_as_production,
            # functools.partial(migrate_datasets, migration="default-units"),
            # functools.partial(migrate_exchanges, migration="default-units"),

        ]
