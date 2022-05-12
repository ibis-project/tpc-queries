import pytest
from ibis_substrait.compiler.core import SubstraitCompiler
from ibis_tpc.substrait import TPCHBackend


@pytest.fixture(scope="module")
def con():
    yield TPCHBackend()


@pytest.fixture(scope="module")
def compiler():
    yield SubstraitCompiler()
