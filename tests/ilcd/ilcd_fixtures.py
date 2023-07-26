from pathlib import Path

import pytest


@pytest.fixture
def example_path():
    example_file_path = (
        Path(__file__).absolute().parent.parent.parent
        / "bw2io/data/examples/ilcd_example.zip"
    )

    return example_file_path
