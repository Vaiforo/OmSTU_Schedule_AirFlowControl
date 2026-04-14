import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

from config import OUT_DIR, TEACHERS_ID


@dag(
    dag_id="omgtu_teacher_scheduler_five",
    start_date=datetime(1900, 1, 1),
    schedule=None,
    catchup=False,
    tags=["omgtu", "schedule"],
)
def dag_teacher_schedule():
    start = EmptyOperator(task_id='start')

    @task
    def get_teachers() -> list[int]:
        return TEACHERS_ID

    @task
    def make_triggers(ids: list[int]) -> list[dict]:
        ctx = get_current_context()
        ds = ctx["ds"]
        ds_nodash = ctx["ds_nodash"]
        parent_run_id = ctx["run_id"]

        return [
            {
                "conf": {
                    "person_id": teacher_id,
                    "ds": ds,
                    "ds_nodash": ds_nodash,
                },
                "trigger_run_id": f"teacher_{teacher_id}_{ds_nodash}_from_{parent_run_id}",
            }
            for teacher_id in ids
        ]

    @task
    def process_teacher_schedule(teacher_id: int) -> str | None:
        ctx = get_current_context()
        ds_nodash = ctx["ds_nodash"]
        logical_date = ctx["logical_date"]

        json_path = os.path.join(OUT_DIR, f"schedule_{teacher_id}_{ds_nodash}.json")
        empty_path = os.path.join(OUT_DIR, f"schedule_{teacher_id}_{ds_nodash}.EMPTY")

        if os.path.isfile(empty_path):
            print(f"Для teacher_id={teacher_id} данных нет: {empty_path}")
            return None

        if not os.path.isfile(json_path):
            raise FileNotFoundError(f"Файл JSON не найден: {json_path}")

        year = logical_date.year
        month = logical_date.month
        day = logical_date.day

        hdfs_dir = f"/schedule/year={year}/month={month}/day={day}/teacher_id={teacher_id}"
        hdfs_file = f"{hdfs_dir}/schedule.json"

        hook = WebHDFSHook(webhdfs_conn_id="webhdfs")
        client = hook.get_conn()

        client.makedirs(hdfs_dir)
        hook.load_file(source=json_path, destination=hdfs_file, overwrite=True)

        print(f"Загружено в HDFS: {hdfs_file}")
        return hdfs_file

    finish = EmptyOperator(task_id='finish')

    teachers = get_teachers()
    trigger_payloads = make_triggers(teachers)

    run_scheduler = TriggerDagRunOperator.partial(
        task_id="run_scheduler",
        trigger_dag_id="omgtu_scheduler_one",
        wait_for_completion=True,
        poke_interval=10,
    ).expand_kwargs(trigger_payloads)

    upload_to_hdfs = process_teacher_schedule.expand(teacher_id=teachers)

    start >> run_scheduler >> upload_to_hdfs >> finish


dag_teacher_schedule()