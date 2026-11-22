import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)


@pytest.fixture
def cluster():
    from system.scripts.cluster import Cluster
    with Cluster() as c:
        yield c
