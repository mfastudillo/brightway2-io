from bw2io.strategies import ilcd
import math


def test_unit_conversion():
    """tests that the convert_to_default_units strategy is able to go from 
    non-default ilcd units to default biosphere3 units.
    """
    # case using non-default ilcd units
    ds = {'unit':'g','exchanges':[
    {'unit':'g','amount':1000,'unit_group':'Units of mass','unit_multiplier':1e-3},
    {'unit':'Rutherford','amount':2,'unit_group':'Units of radioactivity','unit_multiplier':1000},
    ]}

    db = [ds]

    db_converted = ilcd.convert_to_default_units(db)
    e1,e2 = db_converted[0]['exchanges']

    assert math.isclose(e1['amount'],1.0),e1
    assert e1['unit'] == 'kilogram',e1
    assert math.isclose(e2['amount'],2000),e2
    assert e2['unit'] == 'kilo Becquerel'