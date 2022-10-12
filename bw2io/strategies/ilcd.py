
def rename_activity_keys(data:list):
    """renames the keys of the activity dict"""

    renaming_act_dict = {'basename':'name'}
    renaming_exchanges_dict = {'basename':'name',
    'Elementary flow':'biosphere',
    'Product flow':'production','Waste flow':'technosphere'}

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