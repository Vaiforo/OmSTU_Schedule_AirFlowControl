DEFAULT_START = "2026.05.11"
DEFAULT_FINISH = "2026.05.17"
DEFAULT_PERSON_ID = 1003026

FLAG_PATH = "/opt/airflow/dags/configs/start_process.conf"
OUT_DIR = "/opt/airflow/dags/data"

TARGETS = [
    {"type": "person", "id": 1003026},  # Гуненков
    {"type": "person", "id": 782898},  # Шарун
    {"type": "person", "id": 1001117},  # Гулянов
    {"type": "person", "id": 36240},  # Мунько
    {"type": "person", "id": 1001142},  # Плескунов

    {"type": "group", "id": 484},  # МО-231
    {"type": "group", "id": 687},  # ФИТ-231
    {"type": "group", "id": 688},  # ФИТ-232
    {"type": "auditorium", "id": 177},  # 8-201
    {"type": "auditorium", "id": 175},  # 8-204
    {"type": "auditorium", "id": 38},  # Г-331
    {"type": "auditorium", "id": 596},  # Г-411
]

WEBHDFS_CONN_ID = "webhdfs"
CLICKHOUSE_CONN_ID = "clickhouse"
CLICKHOUSE_DATABASE = "rasp_omgtu"
CLICKHOUSE_TABLE = "schedule_lessons"

HDFS_SCHEME = "hdfs"
HDFS_HOST = "namenode"
HDFS_PORT = 9000
HDFS_URI = f"{HDFS_SCHEME}://{HDFS_HOST}:{HDFS_PORT}"

BRONZE_BASE_DIR = "/user/airflow/bronze/schedule"
SILVER_BASE_DIR = "/user/airflow/silver/schedule"

BRONZE_ASSET_URI = f"{HDFS_URI}{BRONZE_BASE_DIR}"
