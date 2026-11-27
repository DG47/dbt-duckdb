import os

import pytest
from dbt.adapters.duckdb import DuckDBConnectionManager
from dbt.tests.util import relation_from_name, run_dbt

# DuckDB writes each NULL as the `null` (or `nullstr`) write option, which the
# CSV sniffer cannot detect when the file is read back
model_sql = """
select 1 as id, 'a' as name, 10 as amount
union all select 2, null, null
"""

csv_null_sql = (
    """{{ config(materialized="external", format="csv", options={"null": "NA"}) }}""" + model_sql
)

csv_backslash_n_sql = (
    """{{ config(materialized="external", format="csv", options={"null": "\\\\N"}) }}"""
    + model_sql
)

csv_nullstr_sql = (
    """{{ config(materialized="external", format="csv", options={"NULLSTR": "'NULL'"}) }}"""
    + model_sql
)

csv_null_partitioned_sql = (
    """{{ config(materialized="external", format="csv", options={"null": "NA", "partition_by": "id"}) }}"""
    + model_sql
)

# the sentinel row of an empty model is written as the null string too
csv_null_empty_sql = """
{{ config(materialized="external", format="csv", options={"null": "NA"}) }}
select 1 as id, 'a' as name, 10 as amount where false
"""

# a nullstr in csv_read_options is kept
csv_null_read_options_sql = """
{{ config(
    materialized="external",
    format="csv",
    options={"null": "NA"},
    csv_read_options={"auto_detect": true, "nullstr": "a"},
) }}
select 1 as id, 'a' as name, 10 as amount
union all select 2, 'b', 20
"""

downstream_sql = """
select id, name, amount from {{ ref('csv_null') }}
"""

EXPECTED = [(1, "a", 10), (2, None, None)]


class TestExternalCsvNullString:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, tmp_path_factory):
        extroot = str(tmp_path_factory.getbasetemp() / "csv_null")
        os.mkdir(extroot)
        dbt_profile_target["external_root"] = extroot
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "csv_null.sql": csv_null_sql,
            "csv_backslash_n.sql": csv_backslash_n_sql,
            "csv_nullstr.sql": csv_nullstr_sql,
            "csv_null_partitioned.sql": csv_null_partitioned_sql,
            "csv_null_empty.sql": csv_null_empty_sql,
            "csv_null_read_options.sql": csv_null_read_options_sql,
        }

    def test_null_string(self, project):
        results = run_dbt()
        assert len(results) == 6

        for name in ("csv_null", "csv_backslash_n", "csv_nullstr", "csv_null_partitioned"):
            relation = relation_from_name(project.adapter, name)
            result = project.run_sql(
                f"select id, name, amount from {relation} order by id", fetch="all"
            )
            assert [tuple(row) for row in result] == EXPECTED, name
            # a numeric column with NULLs keeps its type
            amount_type = project.run_sql(
                f"select typeof(amount) from {relation} limit 1", fetch="one"
            )
            assert amount_type[0] in ("INTEGER", "BIGINT"), name

        relation = relation_from_name(project.adapter, "csv_null_empty")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 0

        relation = relation_from_name(project.adapter, "csv_null_read_options")
        result = project.run_sql(
            f"select id, name, amount from {relation} order by id", fetch="all"
        )
        assert [tuple(row) for row in result] == [(1, None, 10), (2, "b", 20)]

        # the files themselves hold the null string
        extroot = project.adapter.external_root()
        with open(os.path.join(extroot, "csv_null.csv")) as f:
            assert f.read().splitlines()[2] == "2,NA,NA"


class TestExternalCsvNullStringUpstream:
    """register_upstream_external_models reads the files back the same way."""

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, tmp_path_factory):
        extroot = str(tmp_path_factory.getbasetemp() / "csv_null_upstream")
        os.mkdir(extroot)
        dbt_profile_target["external_root"] = extroot
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": ["{{ register_upstream_external_models() }}"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "csv_null.sql": csv_null_sql,
            "downstream.sql": downstream_sql,
        }

    def test_downstream_only_run(self, project):
        run_dbt()
        # Force close the :memory: connection
        DuckDBConnectionManager.close_all_connections()
        run_dbt(["run", "--select", "downstream"])

        relation = relation_from_name(project.adapter, "downstream")
        result = project.run_sql(
            f"select id, name, amount from {relation} order by id", fetch="all"
        )
        assert [tuple(row) for row in result] == EXPECTED
