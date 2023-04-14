
import bw2io
from itertools import zip_longest
import pandas as pd
import logging
import math

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

        # # technosphere flows are assigned by default the dbname and code
        # for e in ds.get('exchanges'):
        #     if e['type']=='Product flow':
        #         e['database'] = ds['database']
        #         e['code'] = ds['uuid']


        #         if e['exchanges_internal_id'] == ds['reference_to_reference_flow']:
        #             # production flow
        #             continue

    return data


def rename_activity_keys(data:list):
    """renames the 'name' and exchange 'type' keys (e.g Elementary flow to 
    biosphere)"""

    renaming_act_dict = {'basename':'name'}
    renaming_exchanges_dict = {'basename':'name',
    'Elementary flow':'biosphere', # Exchange between nature (ecosphere) and technosphere, e.g. an emission, resource.
    'Product flow':'technosphere', # Exchange of goods or services within technosphere, with a positive economic/market value.
    'Waste flow':'technosphere', # Exchange of matters within the technosphere, with a economic/market value equal or below "0".
    'Other flow': 'technosphere',}# Exchange of other type, e.g. dummy or modelling support flows.

    for ds in data:
        
        # rename activity keys
        
        ds[renaming_act_dict['basename']] = ds.pop('basename')

        # rename exchanges
        for exchange in ds.get('exchanges'):

            exchange[renaming_exchanges_dict['basename']] = exchange.pop('basename')
            exchange['type'] = renaming_exchanges_dict[exchange['type']]


    return data


def get_activity_unit(data:list):
    """Sets the unit of the activities. Better if units are already corrected"""

    # in ilcd the unit is in the reference product, that should be identified 
    # with an internal reference code.
    unit_found = False
    for ds in data:
        for exchange in ds['exchanges']:
            if exchange.get('exchanges_internal_id') == ds['reference_to_reference_flow']:
                ds['unit'] = exchange['unit']
                ds['exchanges_name'] = exchange['exchanges_name']
                unit_found = True
                break
    assert unit_found,'unit of the activity could not be found. Failed strategy'
    return data

def set_activity_parameters(data:list):
    """adds the activity parameters as a list dicts

    Args:
        data (list): _description_
    """
    keys = ['parameter_name','parameter_comment','parameter_formula',
    'parameter_mean_value','parameter_minimum_value','parameter_maximum_value',
    'parameter_std95','parameter_distrib',
    ]
    for ds in data:
        
        params = [ds[k] for k in keys]

        # params = [ds['parameter_name'],ds['parameter_comment'],
        # ds['parameter_formula'],ds['parameter_mean_value'],
        # ds['parameter_minimum_value'],ds['parameter_maximum_value'],
        # ds['parameter_std95'],]

        # force it to be a list of list in all cases
        params = [alist if isinstance(alist,list) else [alist]for alist in params]

        has_params = any(([p is not None for p in params]))

        if has_params:
            params_reorganised = []
            for name,comment,formula,mean,minimum,maximum,std,distr, in zip_longest(*params):
                d = {'name':name,'comment':comment,'formula':formula,'mean':mean,
                'minimum':minimum,'maximum':maximum,'std':std,'parameter_distrib':distr,  
                }
                params_reorganised.append(d)
            ds['parameters'] = params_reorganised

            # clean the clutter
            for k in keys:
                ds.pop(k, None)

    return data
    
def set_default_location(data:list):
    """assigns a default location (GLO) if none is given"""
    for ds in data:

        ds['location'] = ds.get('location','GLO')

        if ds['location'] is None:
            ds['location']='GLO'

    return data

def set_production_exchange(data:list)->list:
    """changes the exchange type to 'production' for the reference flow, and sets
    the code and database"""

    for ds in data:
        for exchange in ds['exchanges']:
            if exchange.get('exchanges_internal_id') == ds['reference_to_reference_flow']:

                exchange['type'] = 'production' 
                exchange['database'] = ds['database']
                exchange['code'] = ds['code']
    return data

def convert_to_default_units(data:list):
    """convert the data to the defaults units used in brightway. This means 
    scaling the values .. and probably the uncertainty , only in the exchanges.
    The activity unit is picked from the reference flow later.

    Parameters
    ----------
    data : list
        _description_

    Returns
    -------
    _type_
        _description_
    """

    migration_units = bw2io.units.get_default_units_migration_data()
    unit_conversion_dict = {unit[0]:d for unit,d in migration_units['data']}
    # brightway defaults
    default_units = {f['unit'] for f in unit_conversion_dict.values()}
    
    # ilcd defaults
    # https://eplca.jrc.ec.europa.eu/EF-node/unitgroupList.xhtml;jsessionid=C2A25849AC0F1C03FC8DDFED6AC62AA5?stock=default
    default_units_ilcd = {
    'Mass':'kg',
    'Radioactivity':'kBq',
    'Energy':'MJ', 
    'Area*time':'m2*a',
    'Goods transport (mass*distance)':'',
    'Volume':'m3',

    }

    for ds in data:
            
        for e in ds['exchanges']:
            
            if math.isclose(e['unit_multiplier'],1):
                # normalize name
                e['unit'] = bw2io.units.normalize_units(e['unit'])
            else:
                # convert to ilcd default first
                multiplier = e['unit_multiplier']
                new_unit = default_units_ilcd[e['flow property']]
                e['amount'] *= multiplier
                e['unit'] = new_unit
                
                # normalize name
                e['unit'] = bw2io.units.normalize_units(e['unit'])

                # convert from ilcd default to bw default
                if e['unit'] not in default_units:
                    new_unit = unit_conversion_dict[e['unit']]['unit']
                    multiplier = unit_conversion_dict[e['unit']]['multiplier']
                    e['unit'] = new_unit
                    e['amount'] *= multiplier
                else:
                    continue
    

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


def alternative_map_to_biosphere3(data:list,mapping_dict:dict)->list:
    """sets the code and database of biosphere flows using an alternative mapping.

    Args:
        data (list): _description_
        mapping_path (str): path to an excel file with SourceFlowUUID and 
        TargetFlowUUID as columns

    Returns:
        _type_: _description_
    """

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

# def set_production_code_and_database(data:list):

#     for act in data:

#         for e in act['exchanges']:

#             if e['type']=='production':
#                 e['database'] = act['database']
#                 e['code'] = act['code']

#     return data