DEFAULT_START = "2026.02.09"
DEFAULT_FINISH = "2026.02.14"
DEFAULT_PERSON_ID = 1003026

FLAG_PATH = "/opt/airflow/dags/configs/start_process.conf"
OUT_DIR = "/opt/airflow/dags/data"

TEACHERS_ID = [1003026, 782898, 1001117, 36240, 1001142]

WEBHDFS_CONN_ID = "webhdfs"
CLICKHOUSE_CONN_ID = "clickhouse_http"
CLICKHOUSE_DATABASE = "rasp_omgtu"
CLICKHOUSE_TABLE = "schedule_lessons"

HDFS_SCHEME = "hdfs"
HDFS_HOST = "namenode"
HDFS_PORT = 9000
HDFS_URI = f"{HDFS_SCHEME}://{HDFS_HOST}:{HDFS_PORT}"

BRONZE_BASE_DIR = "/user/airflow/bronze/schedule"
SILVER_BASE_DIR = "/user/airflow/silver/schedule"

BRONZE_ASSET_URI = f"{HDFS_URI}{BRONZE_BASE_DIR}"
