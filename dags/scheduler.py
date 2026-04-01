import asyncio
import json
import os
from datetime import datetime

import aiohttp
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator

from config import DEFAULT_FINISH, DEFAULT_PERSON_ID, DEFAULT_START, OUT_DIR


async def get_json_async(url: str) -> list:
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

    return data


def get_json_sync(url: str) -> list:
    return asyncio.run(get_json_async(url))


@dag(
    dag_id="omgtu_scheduler_one",
    start_date=datetime(1900, 1, 1),
    schedule=None,
    catchup=False,
    tags=["omgtu", "respect", "schedule"],
)
def dag_schedule():
    start = EmptyOperator(task_id='start')

    @task
    def get_run_params() -> dict:
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        person_id = int(conf.get("person_id", DEFAULT_PERSON_ID))
        base_ds = conf.get("ds")
        ds_nodash = conf.get("ds_nodash") or ctx["ds_nodash"]

        logical_date: pendulum.DateTime = (
            pendulum.parse(base_ds) if base_ds else ctx["logical_date"]
        )

        week_start = logical_date.start_of("week")
        week_end = week_start.add(days=6)

        start = conf.get("start", week_start.format("YYYY.MM.DD"))
        finish = conf.get("finish", week_end.format("YYYY.MM.DD"))

        params = {
            "person_id": person_id,
            "ds_nodash": ds_nodash,
            "start": start,
            "finish": finish,
        }

        print(f"Получили person_id = {person_id}")
        print(f"week_start={start} week_end={finish}")
        return params

    @task
    def fetch_schedule(run_payload: dict) -> dict:
        person_id = run_payload["person_id"]
        start = run_payload["start"]
        finish = run_payload["finish"]

        url = (
            f"https://rasp.omgtu.ru/api/schedule/person/{person_id}?start={start}&finish={finish}&lng=1"
        )

        print(f"Запрос расписания: {url}")
        schedule = get_json_sync(url)
        return {"params": run_payload, "schedule": schedule}

    @task
    def save_result(payload: dict) -> str:
        params = payload["params"]
        schedule = payload["schedule"]
        person_id = params["person_id"]
        ds_nodash = params["ds_nodash"]

        os.makedirs(OUT_DIR, exist_ok=True)
        json_path = os.path.join(OUT_DIR, f"schedule_{person_id}_{ds_nodash}.json")
        empty_path = os.path.join(OUT_DIR, f"schedule_{person_id}_{ds_nodash}.EMPTY")

        for path in (json_path, empty_path):
            if os.path.exists(path):
                os.remove(path)

        if schedule:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(schedule, f, ensure_ascii=False, indent=2)
            print(f"Сохранено в файл: {json_path}")
            return json_path

        with open(empty_path, "w", encoding="utf-8") as f:
            f.write("no data")
        print(f"Расписания нет, создан маркер: {empty_path}")
        return empty_path

    finish = EmptyOperator(task_id='finish')

    params = get_run_params()
    schedule = fetch_schedule(params)
    result = save_result(schedule)

    start >> params >> schedule >> result >> finish


dag_schedule()