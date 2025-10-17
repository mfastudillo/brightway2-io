import json
import pandas as pd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw2io.strategies.io import tidy_tables
from bw2io.importers.io import IOImporter
import tempfile
import bw2data as bd


@pytest.fixture
def a_and_b_matrices() -> tuple:

    pet_hiot = pd.DataFrame(
        [[1, -2], [0, 1]],  # experiment setting 1 prod to 0
        index=pd.MultiIndex.from_tuples([("DK", "prod1"), ("DK", "prod2")]),
        columns=pd.MultiIndex.from_tuples([("DK", "act1"), ("DK", "act2")]),
    )

    pet_hiot = pet_hiot.astype(pd.SparseDtype("float", 0))

    B = pd.DataFrame(
        [[1, 3], [1, 2], [0, 1], [4, 0]],
        index=pd.Index(
            ["co2_air", "ch4_air", "co2_accelerated_air", "land_occupation"]
        ),
        columns=pd.MultiIndex.from_tuples([("DK", "act1"), ("DK", "act2")]),
    )

    B = B.astype(pd.SparseDtype("float", 0))

    fd = pd.DataFrame.from_dict(
        {
            ("DK", "Household"): {("DK", "prod1"): -11, ("DK", "prod2"): -3},
            ("DK", "Government"): {("DK", "prod1"): -8, ("DK", "prod2"): -4},
            ("DK", "Capital"): {("DK", "prod1"): -4, ("DK", "prod2"): -2},
        }
    )

    Bfd = pd.DataFrame(
        [
            [1, 3],
        ],
        index=pd.Index(
            [
                "co2_air",
            ]
        ),
        columns=pd.MultiIndex.from_tuples([("DK", "Household"), ("DK", "Government")]),
    ).astype(pd.SparseDtype("float", 0))

    pfd = pd.DataFrame((np.eye(fd.shape[1])), index=fd.columns, columns=fd.columns)
    fd_total = pd.concat([fd, pfd])
    fd_total = fd_total.astype(pd.SparseDtype("float", 0))

    extended_hiot = pd.concat([pet_hiot, fd_total], axis=1).fillna(0)
    extended_B = pd.concat([B, Bfd], axis=1).fillna(0)

    return (extended_hiot, extended_B)


@pytest.fixture
def metadata() -> dict:

    metadata_dict = {
        "prod1": {"unit": "kg", "name": "product 1"},
        "prod2": {"unit": "kg", "name": "product 2"},
        "Household": {"unit": "unit", "name": "the household"},
        "Government": {"unit": "unit", "name": "the government"},
        "Capital": {"unit": "unit", "name": "capital investments"},
        "co2_air": {
            "unit": "ton",  # not standard units
            "name": "carbon dioxide",
            "compartment": ("air",),
        },
        "ch4_air": {"unit": "kg", "name": "methane", "compartment": ("air",)},
        "co2_accelerated_air": {
            "unit": "kg",  # additional biosphere flow
            "name": "carbon dioxide accelerated",
            "compartment": ("air",),
        },
        "land_occupation": {
            "unit": "hectare * year",  # non standard composite unit
            "name": "land occupation",
            "compartment": ("natural resource", "land"),
        },
    }

    return metadata_dict


# @bw2test
def test_tidy_tables(a_and_b_matrices: tuple, metadata: dict, tmp_path):

    DB_NAME = "pet_io_db"
    # if there is a main biosphere

    extended_hiot, extended_B = a_and_b_matrices

    # this creates some tidy tables files
    tidy_tables(extended_hiot, extended_B, tmp_path)

    with open(tmp_path / "io_metadata.json", "w") as fp:
        json.dump(metadata, fp, indent=4)

    # FIXME: to be removed
    bd.projects.set_current("test_io_importer")

    biosphere_mapping = {
        "co2_air": "349b29d1-3e58-4c66-98b9-9d1a076efd2e",
        "ch4_air": "0795345f-c7ae-410c-ad25-1845784c75f5",
        "land_occupation": "c7cb5880-4219-4051-9357-10fdd08c6f2b",
    }

    pet_example = IOImporter(tmp_path, DB_NAME, b3mapping=biosphere_mapping)

    pet_example.apply_strategies()
    pet_example.write_database()

    # the non-standard biosphere flows should be in the database
    # it would fail if is not
    bd.Database(f"{DB_NAME} biosphere").get(code="co2_accelerated_air")

    test_db = bd.Database(DB_NAME)

    connected_to_biosphere = bd.config.biosphere in test_db.find_graph_dependents()

    assert connected_to_biosphere, "expected to link to biosphere"

    # what if we don't provide a mapping, all the biosphere flows
    # should be in the biosphere flows.

    DB_NAME_2 = f"{DB_NAME}_1"
    pet_example = IOImporter(
        tmp_path,
        DB_NAME_2,
    )

    pet_example.apply_strategies()
    pet_example.write_database()

    test_db = bd.Database(DB_NAME_2)
    extra_biosphere = bd.Database("{DB_NAME_2} biosphere")

    assert (
        len(extra_biosphere) == 4
    ), "unexpected number of flows in the extra biosphere"

    connected_to_biosphere = bd.config.biosphere in test_db.find_graph_dependents()
    assert (
        not connected_to_biosphere
    ), "if no mapping is provided it shouldn't be connected to biosphere"
