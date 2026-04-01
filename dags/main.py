import os
import json
import asyncio
from datetime import datetime

import aiohttp

from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context


PERSON_ID = 1003026
DEFAULT_START = "2026.02.02"
DEFAULT_FINISH = "2026.02.08"

FLAG_PATH = "/opt/airflow/dags/configs/start_process.conf"
OUT_DIR = "/opt/airflow/dags/data"


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
    dag_id="omstu_schedule",
    start_date=datetime(1900, 1, 1),
    schedule=None,
    catchup=False,
    tags=["omstu", "schedule"],
)
def dag_schedule():
    wait_for_flag = PythonSensor(
        task_id="wait_for_start_flag",
        python_callable=lambda: os.path.exists(FLAG_PATH),
        poke_interval=10,
        timeout=60 * 60 * 6,
        mode="poke",
    )

    @task
    def fetch_schedule() -> list:
        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}

        start = conf.get("start", DEFAULT_START)
        finish = conf.get("finish", DEFAULT_FINISH)

        url = (
            f"https://rasp.omgtu.ru/api/schedule/person/{PERSON_ID}"
            f"?start={start}&finish={finish}&lng=1"
        )

        print(f"Запрос расписания: {url}")
        return get_json_sync(url)

    @task.branch
    def choose_branch(schedule: list) -> str:
        return "save_json" if schedule else "stay_home"

    @task
    def save_json(schedule: list, run_ds_nodash: str) -> str:
        os.makedirs(OUT_DIR, exist_ok=True)
        path = os.path.join(OUT_DIR, f"respect_schedule_{run_ds_nodash}.json")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(schedule, f, ensure_ascii=False, indent=2)

        print(f"Сохранено в файл: {path}")
        return path

    @task
    def stay_home() -> None:
        print("На данной неделе пары отсутсвутют")

    done = EmptyOperator(task_id="done")

    schedule = fetch_schedule()
    branch = choose_branch(schedule)

    saved = save_json(schedule=schedule, run_ds_nodash="{{ ds_nodash }}")

    wait_for_flag >> schedule >> branch
    branch >> saved >> done
    branch >> stay_home() >> done


dag_schedule()