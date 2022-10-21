
import bw2io

def rename_activity_keys(data:list):
    """renames the keys of the activity dict"""

    renaming_act_dict = {'basename':'name'}
    renaming_exchanges_dict = {'basename':'name',
    'Elementary flow':'biosphere',
    'Product flow':'production',
    'Waste flow':'technosphere',
    'Other flow': 'technosphere',}# ¿?

    for ds in data:
        
        # rename activity keys
        
        ds[renaming_act_dict['basename']] = ds.pop('basename')

        # rename exchanges
        for exchange in ds.get('exchanges'):

            exchange[renaming_exchanges_dict['basename']] = exchange.pop('basename')
            exchange['type'] = renaming_exchanges_dict[exchange['type']]


    return data


def get_activity_unit(data:list):
    """in ilcd the unit is in the reference product, that should be identified
    with an internal reference code."""
    
    for ds in data:
        for exchange in ds['exchanges']:
            if exchange.get('exchanges_internal_id') == ds['reference_to_reference_flow']:
                ds['unit'] = exchange['unit']

    return data

def convert_to_default_units(data:list):
    """convert to default units"""

    migration_units = bw2io.units.get_default_units_migration_data()
    unit_conversion_dict = {unit[0]:d for unit,d in migration_units['data']}

    for ds in data:

        ds['unit'] = bw2io.units.normalize_units(ds['unit'])

        if ds['unit'] in unit_conversion_dict:
            ds['unit'] = unit_conversion_dict[ds['unit']]['unit']
            ds['amount'] *= unit_conversion_dict[ds['unit']]['multiplier']
            
        for e in ds['exchanges']:

            e['unit'] = bw2io.units.normalize_units(e['unit'])

            if e['unit'] in unit_conversion_dict:
                e['unit'] = unit_conversion_dict[e['unit']]['unit']
                e['amount'] *= unit_conversion_dict[e['unit']]['multiplier']

    return data

def map_to_biosphere3(data:list):
    """use the mapping file to define code and database of 
    biosphere flows"""
    ilcd_ecoinvent_id_dict = bw2io.data.get_ilcd_biosphere_migration_data()

    for ds in data:
        for e in ds.get('exchanges'):
            if e['type'] =='biosphere':
                try:
                    e['database'] = 'biosphere3'
                    e['code'] = ilcd_ecoinvent_id_dict[e.get('uuid')]
                    #e['input'] = ('biosphere3',ilcd_ecoinvent_id_dict[e.get('uuid')])
                except KeyError:
                    # this is going to be unlinked
                    continue

    return data