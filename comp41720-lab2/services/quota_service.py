import uvicorn
from fastapi import FastAPI, Request
from typing import Dict, Set

app = FastAPI(title="Quota Service", version="1.0")

# Quota storage: tenant_id -> reserved units
quotas: Dict[str, int] = {}
seen_keys: Set[str] = set()

@app.post("/quota/reserve")
async def reserve_quota(request: Request):
    data = await request.json() if await request.body() else {}
    tenant_id = data.get("tenant_id") or data.get("tenant") or data.get("tenantId") or "default"
    doc_id = data.get("document_id") or data.get("documentId") or data.get("doc_id") or "default"
    units = int(data.get("units", data.get("amount", data.get("quantity", 1))))

    idempotency_key = f"reserve:{tenant_id}:{doc_id}"
    if idempotency_key in seen_keys:
        return {"status": "ALREADY_RESERVED", "reserved": quotas.get(tenant_id, 0), "total_reserved": sum(quotas.values())}

    current = quotas.get(tenant_id, 0)
    quotas[tenant_id] = current + units
    seen_keys.add(idempotency_key)
    print(f"[QuotaService] RESERVED {units} unit(s) for '{tenant_id}'. Total: {quotas[tenant_id]}")
    return {"status": "RESERVED", "reserved": quotas[tenant_id], "total_reserved": sum(quotas.values())}

@app.post("/quota/release")
async def release_quota(request: Request):
    data = await request.json() if await request.body() else {}
    tenant_id = data.get("tenant_id") or data.get("tenant") or data.get("tenantId") or "default"
    doc_id = data.get("document_id") or data.get("documentId") or data.get("doc_id") or "default"
    units = int(data.get("units", data.get("amount", data.get("quantity", 1))))

    idempotency_key = f"release:{tenant_id}:{doc_id}"
    if idempotency_key in seen_keys:
        return {"status": "ALREADY_RELEASED", "reserved": quotas.get(tenant_id, 0), "total_reserved": sum(quotas.values())}

    current = quotas.get(tenant_id, 0)
    quotas[tenant_id] = max(0, current - units)
    seen_keys.add(idempotency_key)
    print(f"[QuotaService] RELEASED {units} unit(s) for '{tenant_id}'. Total: {quotas[tenant_id]}")
    return {"status": "RELEASED", "reserved": quotas[tenant_id], "total_reserved": sum(quotas.values())}

# Harness looks for /quota to check that total reserved usage returned to initial level
@app.get("/quota")
def get_all_quota():
    return {"reserved_quota": sum(quotas.values()), "tenants": quotas}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)