import functools

from .base_lci import LCIImporter
from ..extractors.ilcd import ILCDExtractor
from ..strategies.ilcd import (rename_activity_keys, set_activity_unit,
convert_to_default_units,map_to_biosphere3, set_default_location,
set_production_exchange,setdb_and_code,remove_clutter)
from ..strategies.generic import assign_only_product_as_production
from ..strategies.migrations import migrate_exchanges,migrate_datasets


class ILCDImporter(LCIImporter):
    def __init__(self, dirpath, dbname):

        self.db_name = dbname
        self.data = ILCDExtractor._extract(dirpath)
        self.data = setdb_and_code(self.data,dbname)
        
        self.strategies = [
            rename_activity_keys,
            set_production_exchange,
            convert_to_default_units,
            set_activity_unit,
            assign_only_product_as_production,
            map_to_biosphere3,
            set_default_location,
            remove_clutter,
        ]


