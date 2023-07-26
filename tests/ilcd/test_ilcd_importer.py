from ilcd_fixtures import example_path

from bw2io.importers.ilcd import ILCDImporter
from bw2io.units import normalize_units


def test_examplepath(example_path):
    assert example_path.is_file(), example_path


def test_importer(example_path):
    """test that importer and strategies are working as expected"""
    so = ILCDImporter(example_path, "example_ilcd")
    so.apply_strategies()

    assert type(so.data) is list, "wrong data type"

    for ds in so.data:
        assert isinstance(ds, dict), "wrong data type"

        assert "database" in ds
        assert "code" in ds
        assert "name" in ds
        assert "uuid" in ds
        assert "general_comment" in ds
        assert "location" in ds
        assert "unit" in ds, "missing unit"
        assert "parameters" in ds, "missing parameters"

        for e in ds.get("exchanges"):
            assert "name" in e, e
            assert "type" in e, e
            assert "unit" in e, e

            assert (
                normalize_units(e["unit"]) == e["unit"]
            ), f"no default units {e['unit']}"
