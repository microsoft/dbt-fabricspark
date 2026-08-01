"""Contract enforcement over complex Spark types (ARRAY / MAP / STRUCT).

``data_type_code_to_name`` used to assume the Livy cursor reported column types
as strings. Complex types arrive as nested JSON schema dicts instead, so any
contracted model with an ``array``/``map``/``struct`` column aborted the run
with ``'dict' object has no attribute '__name__'``.

The whole of ``test_constraints.py`` is skipped (``ALTER SET NULL`` is
unsupported on Fabric Spark), so these are the only live-Fabric assertions that
contract enforcement works at all.
"""

import pytest

from dbt.tests.util import run_dbt

_MODEL = """
select
    1 as order_id,
    array('blue', 'green') as tag_list,
    map('priority', 1, 'retries', 0) as attributes,
    named_struct('city', 'Melbourne', 'postcode', 3000) as shipping_address,
    array(named_struct('sku', 'A-1', 'qty', 2)) as line_items
"""

_SCHEMA = """
version: 2
models:
  - name: complex_contract
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: int
      - name: tag_list
        data_type: array<string>
      - name: attributes
        data_type: map<string,int>
      - name: shipping_address
        data_type: struct<city:string,postcode:int>
      - name: line_items
        data_type: array<struct<sku:string,qty:int>>
"""

_SCHEMA_WRONG_ELEMENT_TYPE = """
version: 2
models:
  - name: complex_contract
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: int
      - name: tag_list
        data_type: array<int>
      - name: attributes
        data_type: map<string,int>
      - name: shipping_address
        data_type: struct<city:string,postcode:int>
      - name: line_items
        data_type: array<struct<sku:string,qty:int>>
"""


class TestComplexTypeContractEnforced:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "complex_contract.sql": _MODEL,
            "schema.yml": _SCHEMA,
        }

    def test_contract_with_complex_types_builds(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1


class TestComplexTypeContractElementTypeMismatch:
    """A mismatched element type must still fail the contract.

    Rendering only the top-level kind (``ARRAY``) would make every array
    interchangeable and silently pass this case.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "complex_contract.sql": _MODEL,
            "schema.yml": _SCHEMA_WRONG_ELEMENT_TYPE,
        }

    def test_element_type_mismatch_is_rejected(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        message = results[0].message or ""
        assert "contract" in message.lower()
