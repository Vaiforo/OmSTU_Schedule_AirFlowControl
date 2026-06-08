import json
import os
import uuid

import pika


RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))

REQUEST_QUEUE = "reschedule_requests"


message = {
    "request_id": str(uuid.uuid4()),

    "teacher_id": 1003026,
    "group_id": 484,
    "auditorium_id": 175,

    "subject": "Технологии больших данных",
    "start_date": "2026-05-11",
    "end_date": "2026-05-17",
}


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT)
)
channel = connection.channel()

channel.queue_declare(queue=REQUEST_QUEUE, durable=True)

channel.basic_publish(
    exchange="",
    routing_key=REQUEST_QUEUE,
    body=json.dumps(message, ensure_ascii=False).encode("utf-8"),
    properties=pika.BasicProperties(
        delivery_mode=2,
        content_type="application/json",
    ),
)

print("Сообщение отправлено:")
print(json.dumps(message, ensure_ascii=False, indent=2))

connection.close()
