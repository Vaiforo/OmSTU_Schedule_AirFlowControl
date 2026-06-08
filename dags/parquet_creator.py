from __future__ import annotations

from datetime import datetime

import clickhouse_connect
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag, task
from airflow.sdk.bases.hook import BaseHook
from pyspark.sql import SparkSession, functions as F

from config import (
    BRONZE_ASSET_URI,
    CLICKHOUSE_CONN_ID,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_TABLE,
    HDFS_URI,
    SILVER_BASE_DIR,
)

BRONZE_SCHEDULE_ASSET = Asset(uri=BRONZE_ASSET_URI, name="bronze_schedule")

CLICKHOUSE_PARQUET_STRUCTURE = """
processing_date Date,
target_type String,
target_id UInt32,
ingested_at DateTime,
lesson_oid Nullable(UInt64),
lesson_date Nullable(Date),
day_of_week Nullable(UInt8),
begin_lesson String,
end_lesson String,
lesson_number_start Nullable(UInt8),
lesson_number_end Nullable(UInt8),
duration Nullable(UInt8),
discipline String,
discipline_oid Nullable(UInt32),
kind_of_work String,
kind_of_work_oid Nullable(UInt16),
lecturer String,
lecturer_oid Nullable(UInt32),
subgroup String,
subgroup_oid Nullable(UInt32),
building String,
building_oid Nullable(UInt32),
auditorium String,
auditorium_oid Nullable(UInt32)
""".strip().replace("\n", " ")


def safe_int(column_name: str):
    return F.expr(
        f"try_cast(nullif(trim(cast(`{column_name}` as string)), '') as int)"
    )


def safe_long(column_name: str):
    return F.expr(
        f"try_cast(nullif(trim(cast(`{column_name}` as string)), '') as bigint)"
    )


def with_hdfs_prefix(path: str) -> str:
    if path.startswith("hdfs://"):
        return path
    return f"{HDFS_URI}{path}"


@dag(
    dag_id="omgtu_schedule_silver_clickhouse",
    start_date=datetime(1900, 1, 1),
    schedule=[BRONZE_SCHEDULE_ASSET],
    catchup=False,
    tags=["omgtu", "silver", "clickhouse", "asset"],
)
def dag_silver_clickhouse():
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task(inlets=[BRONZE_SCHEDULE_ASSET])
    def get_asset_event_context(*, inlet_events) -> dict:
        events = inlet_events[BRONZE_SCHEDULE_ASSET]

        if not events:
            raise AirflowSkipException("Нет событий Bronze asset")

        event = events[-1]
        extra = event.extra or {}

        processing_date = extra.get("processing_date")
        bronze_day_path = extra.get("bronze_day_path")
        files = extra.get("files") or []

        if not processing_date or not bronze_day_path:
            raise ValueError(f"Недостаточно данных в asset event extra: {extra}")

        processing_dt = datetime.strptime(processing_date, "%Y-%m-%d")
        year = processing_dt.year
        month = processing_dt.month
        day = processing_dt.day

        bronze_paths = [with_hdfs_prefix(path) for path in files if path]

        payload = {
            "processing_date": processing_date,
            "bronze_day_path": bronze_day_path,
            "bronze_paths": bronze_paths,
            "bronze_glob": f"{HDFS_URI}{bronze_day_path}/type=*/id=*/schedule.json",
            "silver_root_uri": f"{HDFS_URI}{SILVER_BASE_DIR}",
            "silver_partition_glob": (
                f"{HDFS_URI}{SILVER_BASE_DIR}/year={year}/month={month}/day={day}/*.parquet"
            ),
            "year": year,
            "month": month,
            "day": day,
            "week_start": extra.get("week_start"),
            "week_end": extra.get("week_end"),
            "files_count": extra.get("files_count", 0),
        }

        print(f"Контекст обработки asset event: {payload}")
        return payload

    @task
    def build_silver_parquet(payload: dict) -> dict:
        spark = (
            SparkSession.builder.appName("omgtu_schedule_silver")
            .master("local[*]")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.hadoop.fs.defaultFS", HDFS_URI)
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        try:
            read_source = (
                payload["bronze_paths"]
                if payload["bronze_paths"]
                else payload["bronze_glob"]
            )

            raw_df = (
                spark.read.option("multiLine", True)
                .json(read_source)
                .withColumn("bronze_file_path", F.input_file_name())
            )

            if not raw_df.head(1):
                raise AirflowSkipException("В Bronze нет JSON-файлов для преобразования")

            processing_date_col = F.to_date(F.lit(payload["processing_date"]))

            target_type_col = F.regexp_extract(
                F.col("bronze_file_path"),
                r"type=([^/]+)",
                1,
            )

            target_id_match = F.regexp_extract(
                F.col("bronze_file_path"),
                r"id=(\d+)",
                1,
            )

            target_id_col = (
                F.when(F.length(target_id_match) > 0, target_id_match.cast("int"))
                .otherwise(F.lit(None).cast("int"))
            )

            silver_df = (
                raw_df.select(
                    processing_date_col.alias("processing_date"),
                    target_type_col.alias("target_type"),
                    target_id_col.alias("target_id"),
                    F.current_timestamp().alias("ingested_at"),
                    safe_long("lessonOid").alias("lesson_oid"),
                    F.to_date(F.col("date"), "yyyy.MM.dd").alias("lesson_date"),
                    safe_int("dayOfWeek").alias("day_of_week"),
                    F.coalesce(F.col("beginLesson"), F.lit("")).cast("string").alias("begin_lesson"),
                    F.coalesce(F.col("endLesson"), F.lit("")).cast("string").alias("end_lesson"),
                    safe_int("lessonNumberStart").alias("lesson_number_start"),
                    safe_int("lessonNumberEnd").alias("lesson_number_end"),
                    safe_int("duration").alias("duration"),
                    F.coalesce(F.col("discipline"), F.lit("")).cast("string").alias("discipline"),
                    safe_int("disciplineOid").alias("discipline_oid"),
                    F.coalesce(F.col("kindOfWork"), F.lit("")).cast("string").alias("kind_of_work"),
                    safe_int("kindOfWorkOid").alias("kind_of_work_oid"),
                    F.coalesce(F.col("lecturer"), F.lit("")).cast("string").alias("lecturer"),
                    safe_int("lecturerOid").alias("lecturer_oid"),
                    F.coalesce(F.col("subGroup"), F.lit("")).cast("string").alias("subgroup"),
                    safe_int("subGroupOid").alias("subgroup_oid"),
                    F.coalesce(F.col("building"), F.lit("")).cast("string").alias("building"),
                    safe_int("buildingOid").alias("building_oid"),
                    F.coalesce(F.col("auditorium"), F.lit("")).cast("string").alias("auditorium"),
                    safe_int("auditoriumOid").alias("auditorium_oid"),
                )
                .filter(F.col("target_id").isNotNull())
                .dropDuplicates(
                    [
                        "lesson_oid",
                        "lesson_date",
                        "begin_lesson",
                        "end_lesson",
                        "lecturer_oid",
                        "subgroup_oid",
                        "auditorium_oid",
                    ]
                )
                .withColumn("year", F.year("processing_date"))
                .withColumn("month", F.month("processing_date"))
                .withColumn("day", F.dayofmonth("processing_date"))
            )

            if not silver_df.head(1):
                raise AirflowSkipException(
                    "После очистки и преобразования не осталось строк для записи"
                )

            rows_count = silver_df.count()

            (
                silver_df.write.mode("overwrite")
                .partitionBy("year", "month", "day")
                .parquet(payload["silver_root_uri"])
            )

            print(
                "Silver parquet сохранен в HDFS: "
                f"{payload['silver_root_uri']} (обработано строк: {rows_count})"
            )

            payload["rows_count"] = rows_count
            return payload
        finally:
            spark.stop()

    @task
    def load_to_clickhouse(payload: dict) -> str:
        conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)

        client = clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port or 8123,
            username=conn.login or "default",
            password=conn.password or "",
            database=conn.schema or CLICKHOUSE_DATABASE,
        )

        create_database_sql = f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}"

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
        (
            processing_date Date,
            target_type String,
            target_id UInt32,
            ingested_at DateTime,
            lesson_oid Nullable(UInt64),
            lesson_date Nullable(Date),
            day_of_week Nullable(UInt8),
            begin_lesson String,
            end_lesson String,
            lesson_number_start Nullable(UInt8),
            lesson_number_end Nullable(UInt8),
            duration Nullable(UInt8),
            discipline String,
            discipline_oid Nullable(UInt32),
            kind_of_work String,
            kind_of_work_oid Nullable(UInt16),
            lecturer String,
            lecturer_oid Nullable(UInt32),
            subgroup String,
            subgroup_oid Nullable(UInt32),
            building String,
            building_oid Nullable(UInt32),
            auditorium String,
            auditorium_oid Nullable(UInt32)
        )
        ENGINE = MergeTree
        PARTITION BY processing_date
        ORDER BY (
            ifNull(lesson_date, toDate('1970-01-01')),
            ifNull(lecturer_oid, toUInt32(0)),
            ifNull(lesson_oid, toUInt64(0)),
            ifNull(subgroup_oid, toUInt32(0)),
            ifNull(auditorium_oid, toUInt32(0))
        )
        """

        drop_partition_sql = (
            f"ALTER TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} "
            f"DROP PARTITION '{payload['processing_date']}'"
        )

        insert_sql = f"""
        INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
        SELECT
            processing_date,
            target_type,
            target_id,
            ingested_at,
            lesson_oid,
            lesson_date,
            day_of_week,
            begin_lesson,
            end_lesson,
            lesson_number_start,
            lesson_number_end,
            duration,
            discipline,
            discipline_oid,
            kind_of_work,
            kind_of_work_oid,
            lecturer,
            lecturer_oid,
            subgroup,
            subgroup_oid,
            building,
            building_oid,
            auditorium,
            auditorium_oid
        FROM hdfs(
            '{payload["silver_partition_glob"]}',
            'Parquet',
            '{CLICKHOUSE_PARQUET_STRUCTURE}'
        )
        """

        client.command(create_database_sql)
        client.command(create_table_sql)

        existing_partition = client.query(
            f"""
            SELECT count()
            FROM system.parts
            WHERE database = '{CLICKHOUSE_DATABASE}'
              AND table = '{CLICKHOUSE_TABLE}'
              AND active = 1
              AND partition = '{payload["processing_date"]}'
            """
        ).first_row[0]

        if existing_partition:
            client.command(drop_partition_sql)

        client.command(insert_sql)

        message = (
            f"Данные за {payload['processing_date']} загружены в "
            f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"
        )
        print(message)
        return message

    asset_context = get_asset_event_context()
    silver_ready = build_silver_parquet(asset_context)
    clickhouse_loaded = load_to_clickhouse(silver_ready)

    start >> asset_context >> silver_ready >> clickhouse_loaded >> finish


dag_silver_clickhouse()
