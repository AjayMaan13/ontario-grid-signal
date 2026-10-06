"""Checks every DAG file without running it. Needs Airflow installed: make dag-test."""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_every_dag_file_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_the_reconciliation_dag_is_there_with_its_five_tasks_in_order(dagbag):
    dag = dagbag.dags["reconcile_ieso_revisions"]
    assert [t.task_id for t in dag.topological_sort()] == ["find_changes", "fetch_and_archive", "republish", "verify_landed", "record_run_summary"]


def test_every_dag_chooses_catchup_explicitly_and_has_an_owner_and_retries(dagbag):
    for dag in dagbag.dags.values():
        assert dag.catchup is False, f"{dag.dag_id}: set catchup explicitly"
        for task in dag.tasks:
            assert task.owner != "airflow", f"{dag.dag_id}/{task.task_id}: no real owner"
            assert task.retries >= 1, f"{dag.dag_id}/{task.task_id}: no retries"


def test_the_summary_task_runs_even_when_an_earlier_task_failed(dagbag):
    assert dagbag.dags["reconcile_ieso_revisions"].get_task("record_run_summary").trigger_rule.value == "all_done"


def test_only_one_run_at_a_time(dagbag):
    assert dagbag.dags["reconcile_ieso_revisions"].max_active_runs == 1
