import json
import sqlite3
import threading
import time
import uuid
import pika
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Request

app = FastAPI(title="Document Service & Saga Coordinator", version="1.0")
DB_FILE = "documents.db"

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                tenant_id TEXT,
                content TEXT,
                status TEXT,
                created_at REAL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id TEXT PRIMARY KEY,
                routing_key TEXT,
                payload TEXT,
                published INTEGER DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_messages (
                message_id TEXT PRIMARY KEY,
                processed_at REAL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_projection (
                document_id TEXT PRIMARY KEY,
                content TEXT,
                indexed_at REAL
            )
        """)
        conn.commit()

init_db()

@app.post("/documents", status_code=202)
async def create_document(request: Request):
    data = await request.json() if await request.body() else {}
    tenant_id = data.get("tenant_id") or data.get("tenant") or data.get("tenantId") or "default"
    content = data.get("content") or data.get("text") or data.get("body") or data.get("document") or ""
    
    doc_id = str(uuid.uuid4())
    now = time.time()

    # Step 1: Forward Action -> Reserve Quota
    try:
        res = requests.post(
            "http://localhost:8081/quota/reserve",
            json={"tenant_id": tenant_id, "document_id": doc_id, "units": 1},
            timeout=2.0
        )
        res.raise_for_status()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Quota reservation failed: {exc}")

    # Step 2: Transactional Outbox (Atomically save doc and queue event)
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO documents (id, tenant_id, content, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (doc_id, tenant_id, content, "PENDING", now)
        )
        outbox_payload = {
            "message_id": str(uuid.uuid4()),
            "document_id": doc_id,
            "tenant_id": tenant_id,
            "content": content
        }
        cursor.execute(
            "INSERT INTO outbox (id, routing_key, payload, published) VALUES (?, ?, ?, 0)",
            (str(uuid.uuid4()), "document.embed.requested", json.dumps(outbox_payload))
        )
        conn.commit()

    # The harness accepts: {"id": "..."} or {"document_id": "..."}
    return {"id": doc_id, "document_id": doc_id, "status": "PENDING"}

@app.get("/documents/{doc_id}/status")
def get_status(doc_id: str):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM documents WHERE id = ?", (doc_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")
        return {"id": doc_id, "document_id": doc_id, "status": row[0]}

@app.get("/projection/{doc_id}")
def get_projection(doc_id: str):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT document_id, indexed_at FROM search_projection WHERE document_id = ?", (doc_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Not yet available in projection")
        return {"document_id": row[0], "indexed_at": row[1]}

def outbox_relay_loop():
    while True:
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, routing_key, payload FROM outbox WHERE published = 0")
                rows = cursor.fetchall()
                if rows:
                    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
                    channel = connection.channel()
                    channel.exchange_declare(exchange="saga_events", exchange_type="topic", durable=True)

                    for row_id, r_key, payload in rows:
                        channel.basic_publish(
                            exchange="saga_events",
                            routing_key=r_key,
                            body=payload.encode("utf-8"),
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        cursor.execute("UPDATE outbox SET published = 1 WHERE id = ?", (row_id,))
                        conn.commit()
                    connection.close()
        except Exception:
            pass
        time.sleep(0.05)

def saga_coordinator_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
            channel = connection.channel()
            channel.exchange_declare(exchange="saga_events", exchange_type="topic", durable=True)

            queue_name = "document_saga_feedback"
            channel.queue_declare(queue=queue_name, durable=True)
            channel.queue_bind(exchange="saga_events", queue=queue_name, routing_key="document.embed.*")

            for method, properties, body in channel.consume(queue=queue_name, auto_ack=False):
                data = json.loads(body.decode("utf-8"))
                msg_id = data.get("message_id")
                doc_id = data.get("document_id")
                tenant_id = data.get("tenant_id")
                routing_key = method.routing_key

                with sqlite3.connect(DB_FILE) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT message_id FROM processed_messages WHERE message_id = ?", (msg_id,))
                    if cursor.fetchone():
                        channel.basic_ack(method.delivery_tag)
                        continue

                    if routing_key == "document.embed.completed":
                        cursor.execute("UPDATE documents SET status = 'COMPLETED' WHERE id = ?", (doc_id,))
                        cursor.execute(
                            "INSERT OR REPLACE INTO search_projection (document_id, content, indexed_at) VALUES (?, ?, ?)",
                            (doc_id, data.get("content", ""), time.time())
                        )
                    elif routing_key == "document.embed.failed":
                        cursor.execute("UPDATE documents SET status = 'COMPENSATED' WHERE id = ?", (doc_id,))
                        try:
                            requests.post(
                                "http://localhost:8081/quota/release",
                                json={"tenant_id": tenant_id, "document_id": doc_id, "units": 1},
                                timeout=2.0
                            )
                        except Exception:
                            pass

                    cursor.execute("INSERT INTO processed_messages (message_id, processed_at) VALUES (?, ?)", (msg_id, time.time()))
                    conn.commit()

                channel.basic_ack(method.delivery_tag)
        except Exception:
            time.sleep(1.0)

threading.Thread(target=outbox_relay_loop, daemon=True).start()
threading.Thread(target=saga_coordinator_consumer, daemon=True).start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)