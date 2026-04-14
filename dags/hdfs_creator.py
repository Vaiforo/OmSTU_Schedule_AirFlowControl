import os
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.sdk import Asset

from config import BRONZE_ASSET_URI, BRONZE_BASE_DIR, OUT_DIR, TEACHERS_ID, WEBHDFS_CONN_ID

BRONZE_SCHEDULE_ASSET = Asset(uri=BRONZE_ASSET_URI, name="bronze_schedule")


@dag(
    dag_id="omgtu_teacher_scheduler_five",
    start_date=datetime(1900, 1, 1),
    schedule=None,
    catchup=False,
    tags=["omgtu", "schedule", "bronze"],
)
def dag_teacher_schedule():
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task
    def get_teachers() -> list[int]:
        return TEACHERS_ID

    @task
    def make_triggers(ids: list[int]) -> list[dict]:
        ctx = get_current_context()
        logical_date = ctx["logical_date"]
        ds_nodash = ctx["ds_nodash"]
        parent_run_id = ctx["run_id"]

        week_start = logical_date.start_of("week").format("YYYY.MM.DD")
        week_end = logical_date.start_of(
            "week").add(days=6).format("YYYY.MM.DD")
        processing_date = logical_date.strftime("%Y-%m-%d")

        return [
            {
                "conf": {
                    "person_id": teacher_id,
                    "processing_date": processing_date,
                    "ds_nodash": ds_nodash,
                    "start": week_start,
                    "finish": week_end,
                },
                "trigger_run_id": f"teacher_{teacher_id}_{ds_nodash}_from_{parent_run_id}",
            }
            for teacher_id in ids
        ]

    @task
    def process_teacher_schedule(teacher_id: int) -> str | None:
        ctx = get_current_context()
        ds_nodash = ctx["ds_nodash"]
        logical_date: pendulum.DateTime = ctx["logical_date"]

        json_path = os.path.join(
            OUT_DIR, f"schedule_{teacher_id}_{ds_nodash}.json")
        empty_path = os.path.join(
            OUT_DIR, f"schedule_{teacher_id}_{ds_nodash}.EMPTY")

        if os.path.isfile(empty_path):
            print(f"Для teacher_id={teacher_id} данных нет: {empty_path}")
            return None

        if not os.path.isfile(json_path):
            raise FileNotFoundError(f"Файл JSON не найден: {json_path}")

        year = logical_date.year
        month = logical_date.month
        day = logical_date.day

        hdfs_dir = f"{BRONZE_BASE_DIR}/year={year}/month={month}/day={day}/teacher_id={teacher_id}"
        hdfs_file = f"{hdfs_dir}/schedule.json"

        hook = WebHDFSHook(webhdfs_conn_id=WEBHDFS_CONN_ID)
        client = hook.get_conn()

        client.makedirs(hdfs_dir)
        hook.load_file(source=json_path, destination=hdfs_file, overwrite=True)

        print(f"Загружено в HDFS: {hdfs_file}")
        return hdfs_file

    @task(outlets=[BRONZE_SCHEDULE_ASSET])
    def publish_bronze_ready(hdfs_files: list[str | None], *, outlet_events) -> dict:
        actual_files = [path for path in hdfs_files if path]
        if not actual_files:
            raise AirflowSkipException(
                "Нет JSON-файлов в Bronze за этот запуск")

        ctx = get_current_context()
        logical_date: pendulum.DateTime = ctx["logical_date"]

        processing_date = logical_date.to_date_string()
        year = logical_date.year
        month = logical_date.month
        day = logical_date.day
        week_start = logical_date.start_of("week").format("YYYY.MM.DD")
        week_end = logical_date.start_of(
            "week").add(days=6).format("YYYY.MM.DD")

        bronze_day_path = f"{BRONZE_BASE_DIR}/year={year}/month={month}/day={day}"
        event_extra = {
            "processing_date": processing_date,
            "year": year,
            "month": month,
            "day": day,
            "week_start": week_start,
            "week_end": week_end,
            "bronze_day_path": bronze_day_path,
            "files_count": len(actual_files),
            "files": actual_files,
        }
        outlet_events[BRONZE_SCHEDULE_ASSET].extra = event_extra
        print(f"Опубликовано событие Bronze asset: {event_extra}")
        return event_extra

    teachers = get_teachers()
    trigger_payloads = make_triggers(teachers)

    run_scheduler = TriggerDagRunOperator.partial(
        task_id="run_scheduler",
        trigger_dag_id="omgtu_scheduler_one",
        wait_for_completion=True,
        poke_interval=10,
    ).expand_kwargs(trigger_payloads)

    upload_to_hdfs = process_teacher_schedule.expand(teacher_id=teachers)
    bronze_ready = publish_bronze_ready(upload_to_hdfs)

    start >> teachers >> trigger_payloads >> run_scheduler >> upload_to_hdfs >> bronze_ready >> finish


dag_teacher_schedule()
