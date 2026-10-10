import json
import os
import random
import threading
import time
import uuid
import pika
import uvicorn
from fastapi import FastAPI

app = FastAPI(title="Embedding Service", version="1.0")

processed_embeddings = set()

def embedding_worker():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
            channel = connection.channel()
            channel.exchange_declare(exchange="saga_events", exchange_type="topic", durable=True)

            queue_name = "embedding_requests"
            channel.queue_declare(queue=queue_name, durable=True)
            channel.queue_bind(exchange="saga_events", queue=queue_name, routing_key="document.embed.requested")

            for method, properties, body in channel.consume(queue=queue_name, auto_ack=False):
                data = json.loads(body.decode("utf-8"))
                msg_id = data.get("message_id")
                doc_id = data.get("document_id")
                tenant_id = data.get("tenant_id")

                # Idempotency guard
                if msg_id in processed_embeddings:
                    channel.basic_ack(method.delivery_tag)
                    continue

                failure_rate = float(os.getenv("FAILURE_RATE", "0.0"))
                # Simulated embedding computation
                time.sleep(0.2)

                response_msg_id = str(uuid.uuid4())
                if random.random() < failure_rate:
                    err_payload = {
                        "message_id": response_msg_id,
                        "document_id": doc_id,
                        "tenant_id": tenant_id,
                        "reason": "Simulated embedding inference failure"
                    }
                    channel.basic_publish(
                        exchange="saga_events",
                        routing_key="document.embed.failed",
                        body=json.dumps(err_payload).encode("utf-8"),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    print(f"[EmbeddingService] Injected failure for Doc {doc_id} -> published embed.failed")
                else:
                    ok_payload = {
                        "message_id": response_msg_id,
                        "document_id": doc_id,
                        "tenant_id": tenant_id,
                        "content": data.get("content"),
                        "vector": [0.101, 0.202, 0.303]
                    }
                    channel.basic_publish(
                        exchange="saga_events",
                        routing_key="document.embed.completed",
                        body=json.dumps(ok_payload).encode("utf-8"),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    print(f"[EmbeddingService] Completed embedding for Doc {doc_id} -> published embed.completed")

                processed_embeddings.add(msg_id)
                channel.basic_ack(method.delivery_tag)
        except Exception:
            time.sleep(2.0)

threading.Thread(target=embedding_worker, daemon=True).start()

@app.get("/health")
def health():
    return {"status": "UP", "failure_rate": float(os.getenv("FAILURE_RATE", "0.0"))}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)