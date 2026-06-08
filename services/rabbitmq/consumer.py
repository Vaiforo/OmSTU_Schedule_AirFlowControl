import json
import os
from datetime import datetime, timedelta

import clickhouse_connect
import pika


RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))

REQUEST_QUEUE = "reschedule_requests"
RESPONSE_QUEUE = "reschedule_responses"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "airflow")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "rasp_omgtu")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "schedule_lessons")

MAX_GROUP_LESSONS_PER_DAY = 4
MAX_RESULTS = 5

LESSON_TIMES = {
    1: ("08:00", "09:30"),
    2: ("09:40", "11:10"),
    3: ("11:35", "13:05"),
    4: ("13:15", "14:45"),
    5: ("15:10", "16:40"),
    6: ("16:50", "18:20"),
}


def date_range(start_date: str, end_date: str):
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    finish = datetime.strptime(end_date, "%Y-%m-%d").date()

    while current <= finish:
        yield current
        current += timedelta(days=1)


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def get_busy_lessons(client, teacher_id, group_id, auditorium_id, start_date, end_date):
    query = f"""
        SELECT
            lesson_date,
            lesson_number_start,
            lesson_number_end,
            lecturer_oid,
            subgroup_oid,
            auditorium_oid
        FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
        WHERE lesson_date BETWEEN toDate(%(start_date)s) AND toDate(%(end_date)s)
          AND (
                lecturer_oid = %(teacher_id)s
             OR subgroup_oid = %(group_id)s
             OR auditorium_oid = %(auditorium_id)s
          )
    """

    result = client.query(
        query,
        parameters={
            "start_date": start_date,
            "end_date": end_date,
            "teacher_id": teacher_id,
            "group_id": group_id,
            "auditorium_id": auditorium_id,
        },
    )

    return result.result_rows


def lesson_numbers_range(lesson_number_start, lesson_number_end):
    if lesson_number_start is None:
        return set()

    start = int(lesson_number_start)
    end = int(lesson_number_end) if lesson_number_end is not None else start

    if end < start:
        end = start

    return set(range(start, end + 1))


def is_busy(rows, day, lesson_number, teacher_id, group_id, auditorium_id):
    teacher_busy = False
    group_busy = False
    auditorium_busy = False

    for row in rows:
        lesson_date, lesson_start, lesson_end, lecturer_oid, subgroup_oid, auditorium_oid = row

        if lesson_date != day:
            continue

        occupied_numbers = lesson_numbers_range(lesson_start, lesson_end)

        if lesson_number not in occupied_numbers:
            continue

        if lecturer_oid == teacher_id:
            teacher_busy = True

        if subgroup_oid == group_id:
            group_busy = True

        if auditorium_oid == auditorium_id:
            auditorium_busy = True

    return teacher_busy, group_busy, auditorium_busy


def get_group_lesson_numbers(rows, day, group_id):
    lesson_numbers = set()

    for row in rows:
        lesson_date, lesson_start, lesson_end, lecturer_oid, subgroup_oid, auditorium_oid = row

        if lesson_date == day and subgroup_oid == group_id:
            lesson_numbers.update(
                lesson_numbers_range(lesson_start, lesson_end))

    return lesson_numbers


def count_group_lessons(rows, day, group_id):
    return len(get_group_lesson_numbers(rows, day, group_id))


def has_bad_window(rows, day, group_id, candidate_lesson_number):
    lesson_numbers = get_group_lesson_numbers(rows, day, group_id)
    lesson_numbers.add(candidate_lesson_number)

    if not lesson_numbers:
        return False

    first_lesson = min(lesson_numbers)
    last_lesson = max(lesson_numbers)

    total_span = last_lesson - first_lesson + 1
    real_lessons = len(lesson_numbers)
    windows = total_span - real_lessons

    return windows > 1


def find_slots(request):
    teacher_id = int(request["teacher_id"])
    group_id = int(request["group_id"])
    auditorium_id = int(request["auditorium_id"])

    start_date = request.get("start_date")
    end_date = request.get("end_date")

    if not start_date or not end_date:
        raise ValueError(
            "В запросе должны быть start_date и end_date в формате YYYY-MM-DD")

    client = get_clickhouse_client()

    rows = get_busy_lessons(
        client=client,
        teacher_id=teacher_id,
        group_id=group_id,
        auditorium_id=auditorium_id,
        start_date=start_date,
        end_date=end_date,
    )

    slots = []

    for day in date_range(start_date, end_date):
        for lesson_number, lesson_time in LESSON_TIMES.items():
            teacher_busy, group_busy, auditorium_busy = is_busy(
                rows=rows,
                day=day,
                lesson_number=lesson_number,
                teacher_id=teacher_id,
                group_id=group_id,
                auditorium_id=auditorium_id,
            )

            if teacher_busy or group_busy or auditorium_busy:
                continue

            group_lessons_count = count_group_lessons(rows, day, group_id)

            if group_lessons_count >= MAX_GROUP_LESSONS_PER_DAY:
                continue

            if has_bad_window(rows, day, group_id, lesson_number):
                continue

            slots.append(
                {
                    "date": str(day),
                    "lesson_number": lesson_number,
                    "begin_lesson": lesson_time[0],
                    "end_lesson": lesson_time[1],
                    "teacher_id": teacher_id,
                    "group_id": group_id,
                    "auditorium_id": auditorium_id,
                }
            )

            if len(slots) >= MAX_RESULTS:
                return slots

    return slots


def publish_response(channel, response: dict):
    channel.basic_publish(
        exchange="",
        routing_key=RESPONSE_QUEUE,
        body=json.dumps(response, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )


def handle_message(channel, method, properties, body):
    request_id = None

    try:
        request = json.loads(body.decode("utf-8"))
        request_id = request.get("request_id")

        slots = find_slots(request)

        response = {
            "request_id": request_id,
            "status": "success",
            "slots_count": len(slots),
            "slots": slots,
        }

        publish_response(channel, response)

        channel.basic_ack(delivery_tag=method.delivery_tag)

        print("Запрос обработан:")
        print(json.dumps(response, ensure_ascii=False, indent=2))

    except Exception as e:
        error_response = {
            "request_id": request_id,
            "status": "error",
            "error": str(e),
        }

        try:
            publish_response(channel, error_response)
        finally:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        print("Ошибка обработки сообщения:")
        print(json.dumps(error_response, ensure_ascii=False, indent=2))


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT)
    )
    channel = connection.channel()

    channel.queue_declare(queue=REQUEST_QUEUE, durable=True)
    channel.queue_declare(queue=RESPONSE_QUEUE, durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=REQUEST_QUEUE,
        on_message_callback=handle_message,
        auto_ack=False,
    )

    print("Consumer запущен. Ожидание сообщений...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
