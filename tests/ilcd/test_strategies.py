from bw2io.strategies import ilcd
import math


def test_unit_conversion():

    # basic transformation to default units
    ds = {'unit':'g','exchanges':[
    {'unit':'g','amount':1000},
    {'unit':'Rutherford','amount':1},
    ]}

    db = [ds]

    db_converted = ilcd.convert_to_default_units(db)
    assert math.isclose(db_converted[0]['exchanges'][0]['amount'],1.0)