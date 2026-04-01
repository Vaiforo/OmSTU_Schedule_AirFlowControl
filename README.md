# OmSTU_Schedule_AirFlowControl
Service built on Apache Airflow for automated collection and processing of OMSTU teacher schedules via API. Data проходит through an ETL pipeline and is stored in a structured format (HDFS), using RabbitMQ for inter-service communication. Includes DAG orchestration, error handling, and inter-task coordination.
