import functools

from .base_lci import LCIImporter
from ..extractors.ilcd import ILCDExtractor
from ..strategies.ilcd import (rename_activity_keys, get_activity_unit,
convert_to_default_units,map_to_biosphere3, set_default_location,set_production_exchange)
from ..strategies.generic import assign_only_product_as_production
from ..strategies.migrations import migrate_exchanges,migrate_datasets


class ILCDImporter(LCIImporter):
    def __init__(self, dirpath, dbname):

        self.db_name = dbname
        self.data = ILCDExtractor._extract(dirpath)
        self.strategies = [
            rename_activity_keys,
            get_activity_unit,
            set_production_exchange,
            convert_to_default_units,
            assign_only_product_as_production,
            map_to_biosphere3,
            set_default_location,

        ]

        # not sure if it should be done here...
        for ds in self.data:
            ds['database'] = dbname
            ds['code'] = ds['uuid']
            ds['type']='process'

            # technosphere flows are assigned by default the dbname and code
            for e in ds.get('exchanges'):
                if e['type']=='Product flow':
                    e['database'] = ds['database']
                    e['code'] = ds['uuid']


                    if e['exchanges_internal_id'] == ds['reference_to_reference_flow']:
                        # production flow
                        continue
