
import bw2io
from itertools import zip_longest

def setdb_and_code(data:list,dbname:str):

    # not sure if it should be done here...
    for ds in data:
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

    return data


def rename_activity_keys(data:list):
    """renames the keys of the activity dict"""

    renaming_act_dict = {'basename':'name'}
    renaming_exchanges_dict = {'basename':'name',
    'Elementary flow':'biosphere',
    'Product flow':'technosphere',
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

def set_activity_parameters(data:list):
    """adds the activity parameters as a list dicts

    Args:
        data (list): _description_
    """
    for ds in data:
        
        params = [ds['parameter_name'],ds['parameter_comment'],
        ds['parameter_formula'],ds['parameter_mean_value'],
        ds['parameter_minimum_value'],ds['parameter_maximum_value'],
        ds['parameter_std95'],]

        params_reorganised = []
        for name,comment,formula,mean,minimum,maximum,std, in zip_longest(*params):
            d = {'name':name,'comment':comment,'formula':formula,'mean':mean,
            'minimum':minimum,'maximum':maximum,'std':std,    
            }
            params_reorganised.append(d)
        ds['parameters'] = params_reorganised

    return data
    
def set_default_location(data:list):

    for ds in data:

        ds['location'] = ds.get('location','GLO')

        if ds['location'] is None:
            ds['location']='GLO'

    return data

def set_production_exchange(data:list):

    for ds in data:
        for exchange in ds['exchanges']:
            if exchange.get('exchanges_internal_id') == ds['reference_to_reference_flow']:

                exchange['type'] = 'production' 
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

                except KeyError:
                    # this is going to be unlinked
                    continue

    return data