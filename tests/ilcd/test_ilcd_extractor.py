from pathlib import Path

import pytest
from ilcd_fixtures import example_path
from lxml.etree import _Element

from bw2io.extractors.ilcd import (apply_xpaths_to_xml_file,
                                   extract_all_relevant_info, extract_zip,
                                   get_contact_from_etree,
                                   get_flows_from_etree, get_xml_value,
                                   namespaces_dict, xpaths,extract)


# fixtures
@pytest.fixture
def example_tree(example_path):

    etrees_dict = extract_zip(example_path)
    return etrees_dict

## tests
def test_examplepath(example_path):

    assert example_path.is_file(),example_path

def test_get_contact_from_etree(example_tree):
    """tests the function getting a contact list from the tree"""

    contacts = get_contact_from_etree(example_tree)
    assert isinstance(contacts,list)
    contact_1,*rest = contacts

    assert 'email' in contact_1
    assert 'website' in contact_1
    assert 'short_name' in contact_1

def test_get_flows_from_etree(example_tree):
    
    """test that get_flows_from_etree gets the right keys

    Args:
        example_tree (_type_): _description_
    """
    xpaths_dicts = xpaths()
    flows = get_flows_from_etree(example_tree)
    assert isinstance(flows,list)
    assert len(flows) > 0

    one_flow = flows[0]
    expeted_keys = xpaths_dicts['xpaths_flows'].keys()

    for key in expeted_keys:
        assert key in one_flow



def test_extract_zip(example_path):

    trees = extract_zip(example_path)
    # assure completeness
    assert 'contacts' in trees
    assert 'flows' in trees
    assert 'processes' in trees

    # assure that all return values are etrees
    for branches in trees:
        assert all(
            [isinstance(trees[branches][t], _Element) for t in trees[branches]]
        )


def test_xml_value_getter(example_path):

    xpaths_dict = xpaths()
    xpaths_process = xpaths_dict['xpaths_process']
    namespaces = namespaces_dict()

    trees = extract_zip(example_path)
    tree_object = trees["processes"][list(trees["processes"])[0]]
    xpath_str = xpaths_process["basename"]
    v = get_xml_value(
        tree_object,
        xpath_str,
        namespaces["default_process_ns"],
        namespaces["others"],
    )
    assert v == 'Light duty vehicle',v

def test_extract(example_path):

    data = extract(example_path)

    for ds in data:

        assert 'reference_to_reference_flow' in ds,'missing ref to ref.flow' # used to identify ref. flow

        for e in ds['exchanges']:
        
            assert 'exchanges_internal_id' in e,'missing internal id' # used to identify ref. flow