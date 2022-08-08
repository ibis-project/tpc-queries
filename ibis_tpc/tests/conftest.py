import ibis
import pytest
from ibis_substrait.compiler.core import SubstraitCompiler
from ibis_tpc.substrait import TPCHBackend


@pytest.fixture(scope="module")
def con():
    yield TPCHBackend()


@pytest.fixture(scope="module")
def compiler():
    yield SubstraitCompiler()


@pytest.fixture(scope="session")
def duckcon():
    con = ibis.duckdb.connect()
    con.con.execute("CALL dbgen(sf=0.1)")
    con.con.execute("install substrait")
    con.con.execute("load substrait")
    yield con
