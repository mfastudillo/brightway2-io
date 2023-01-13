
import bw2io
from itertools import zip_longest
import pandas as pd

def setdb_and_code(data:list,dbname:str)->list:
    """

    Args:
        data (list): _description_
        dbname (str): _description_

    Returns:
        list: _description_
    """

    # not sure if it should be done here...
    for ds in data:
        ds['database'] = dbname
        ds['type']='process' 
        ds['code'] = ds['uuid']

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
    """renames the 'name' and exchange 'type' keys (e.g Elementary flow to biosphere)"""

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
    """Sets the unit of the activities"""

    # in ilcd the unit is in the reference product, that should be identified 
    # with an internal reference code.
    
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

        has_params = any(([p is not None for p in params]))

        if has_params:
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

def set_production_exchange(data:list)->list:
    """changes the exchange type to 'production' for the reference flow"""

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
    """sets the code and database of biosphere flows tryng to link to the
    biosphere3 database. It uses a mapping between ilcd elementary flows and
    ecoinvent elementary flows."""
    ilcd_ecoinvent_id_dict = bw2io.data.get_ilcd_biosphere_migration_data()

    for ds in data:
        for e in ds.get('exchanges'):
            if e['type'] =='biosphere':
                try:
                    e['code'] = ilcd_ecoinvent_id_dict[e.get('uuid')]
                    e['database'] = 'biosphere3'

                except KeyError:
                    # this is going to be unlinked
                    continue

    return data


def alternative_map_to_biosphere3(data:list,mapping_path:str):
    """sets the code and database of biosphere flows using an alternative mapping.

    Args:
        data (list): _description_
        mapping_path (str): path to an excel file with SourceFlowUUID and 
        TargetFlowUUID as columns

    Returns:
        _type_: _description_
    """

    mapping_df = pd.read_excel(mapping_path)
    mapping_dict = mapping_df.set_index('SourceFlowUUID')['TargetFlowUUID'].dropna().to_dict()

    for ds in data:
        for e in ds.get('exchanges'):
            if e['type'] =='biosphere':
                try:
                    e['code'] = mapping_dict[e.get('uuid')]
                    e['database'] = 'biosphere3'

                except KeyError:
                    # this is going to be unlinked
                    continue

    return data