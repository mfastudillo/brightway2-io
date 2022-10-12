from .base_lci import LCIImporter
from ..extractors.ilcd import ILCDExtractor
from ..strategies.ilcd import rename_activity_keys, get_activity_unit
from ..strategies.generic import assign_only_product_as_production


class ILCDImporter(LCIImporter):
    def __init__(self, dirpath, dbname):

        self.dbname = dbname
        self.data = ILCDExtractor._extract(dirpath)
        self.strategies = [
            rename_activity_keys,
            get_activity_unit,
            assign_only_product_as_production,
        ]
