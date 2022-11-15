from bw2io.extractors.ilcd import (
    extract_zip,
    get_xml_value,
    extract_all_relevant_info,
    xpaths_process,
    xpaths_flows,
    namespaces,
    apply_xpaths_to_xml_file,
)

from bw2io.importers.ilcd import ILCDImporter
from bw2io.units import normalize_units
from pathlib import Path
from lxml.etree import _Element
import pytest

example_file = (
    Path(__file__).absolute().parent.parent.parent
    / "bw2io/data/examples/ilcd_example.zip"
)

@pytest.fixture
def example_path():

    example_file_path = (
    Path(__file__).absolute().parent.parent.parent
    / "bw2io/data/examples/ilcd_example.zip")

    return example_file_path

def test_examplepath(example_path):

    assert example_path.is_file(),example_path

def test_importer(example_path):
    """test that importer and strategies are working as expected"""
    so = ILCDImporter(example_path,'example_ilcd')
    so.apply_strategies()

    assert type(so.data) is list,'wrong data type'

    for ds in so.data:

        assert isinstance(ds,dict),'wrong data type'
        assert 'database' in ds
        assert 'code' in ds
        assert 'name' in ds

        for e in ds.get('exchanges'):

            assert 'name' in e,e
            assert 'type' in e,e
            assert 'unit' in e,e

            assert normalize_units(e['unit']) == e['unit'],f"no default units {e['unit']}"



def test_extract_zip():

    trees = extract_zip(example_file)
    # assure completeness
    assert len(trees) == 2

    # assure that all return values are etrees
    for branches in trees:
        assert all(
            [isinstance(trees[branches][t], _Element) for t in trees[branches]]
        )

    return trees


def test_xml_value_getter():
    trees = extract_zip(example_file)
    tree_object = trees["processes"][list(trees["processes"])[0]]
    xpath_str = xpaths_process["basename"]
    v = get_xml_value(
        tree_object,
        xpath_str,
        namespaces["default_process_ns"],
        namespaces["others"],
    )
    assert v == 'Light duty vehicle',v

# values of the previous example
# def test_apply_xpaths_to_xml_file():
#     trees = extract_zip(example_file)
#     tree_object = trees["processes"][list(trees["processes"])[0]]
#     v = apply_xpaths_to_xml_file(xpaths_process, tree_object)
#     v_expected = {
#         "basename": "Aromatic Polyester Polyols (APP) production mix",
#         "treatment_standards_routes": "polycondensation",
#         "mix_and_location_types": "production mix, at producer",
#         "functional_unit_flow_properties": "Hydroxyl value: 150-360 mg KOH/g, aromatic content: 5-50%; Average gross calorific value 22.5 MJ/kg",
#         "uuid": "d2fe899e-7fc0-49d3-a7cc-bbf8cad5439a",
#         "reference_year": "2019",
#         "data_set_valid_until": "2026",
#         "location": "EU-28",
#         "reference_to_reference_flow": None,
#     }
#     # do not compare exchanges because too much data
#     assert all(v_expected[k] == v[k] for k in v_expected)


# def test_extract_all_relevant_info():
#     v = extract_all_relevant_info(example_file)
#     assert "processes" in v
#     assert len(v["processes"]) == 1
#     assert "flows" in v
#     assert len(v["flows"]) == 1240
#     expected_first_flow = {
#         "basename": "Spoil (deposited)",
#         "uuid": "fa1d0ee9-d657-4d0b-9ee4-7a0f5f46d462",
#         "category": None,
#         "type": "Waste flow",
#         "value": "1.0",
#         "refobj": "93a60a56-a3c8-11da-a746-0800200b9a66",
#     }
#     assert v["flows"][0] == expected_first_flow


# def test_lookup_flowproperty():
#     #print(lookup_flowproperty('93a60a56-a3c8-11da-a746-0800200c9a66')[0])
#     assert(lookup_flowproperty('93a60a56-a3c8-11da-a746-0800200c9a66')[0]=='MJ')
#     assert(lookup_flowproperty('93a60a56-a3c8-11da-a746-0800200b9a66')[0]=='kg')
#     assert(lookup_flowproperty('93a60a56-a3c8-11da-a746-0800200c9a66')[1]=='Net calorific value')
#     pass


if __name__ == "__main__":
    test_extract_zip()
    test_xml_value_getter()
    test_apply_xpaths_to_xml_file()
    test_extract_all_relevant_info()
    test_lookup_flowproperty()
