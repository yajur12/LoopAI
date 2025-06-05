# main.py
import uuid
import asyncio
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator
from enum import Enum
from typing import List
from collections import deque
from threading import Lock

app = FastAPI()

class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class IngestRequest(BaseModel):
    ids: List[int]
    priority: Priority

    @validator("ids")
    def validate_ids(cls, v):
        if not v:
            raise ValueError("At least one ID is required")
        for id_ in v:
            if not (1 <= id_ <= 10**9 + 7):
                raise ValueError(f"ID {id_} is out of range (1 to 10^9+7)")
        return v

class BatchStatus(str, Enum):
    YET_TO_START = "yet_to_start"
    TRIGGERED = "triggered"
    COMPLETED = "completed"

# In-memory stores
ingestion_store = {}  # ingestion_id: dict
batch_queue = deque()  # (priority_value, created_time, batch_data)
lock = Lock()

# Priority Mapping
priority_map = {
    Priority.HIGH: 1,
    Priority.MEDIUM: 2,
    Priority.LOW: 3
}

# Constants
BATCH_SIZE = 3
RATE_LIMIT_SECONDS = 5
PROCESS_DELAY = 2  # Simulated processing delay

@app.post("/ingest")
async def ingest(request: IngestRequest):
    try:
        ingestion_id = str(uuid.uuid4())
        ids = request.ids
        priority = request.priority
        created_time = time.time()
        batches = []

        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i:i + BATCH_SIZE]
            batch_id = str(uuid.uuid4())
            batch_info = {
                "batch_id": batch_id,
                "ids": batch_ids,
                "status": BatchStatus.YET_TO_START,
                "ingestion_id": ingestion_id,
                "created_time": created_time,
                "priority": priority
            }
            batches.append(batch_info)

            with lock:
                batch_queue.append((priority_map[priority], created_time, batch_info))

        ingestion_store[ingestion_id] = {
            "status": BatchStatus.YET_TO_START,
            "batches": batches
        }

        return {"ingestion_id": ingestion_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/status/{ingestion_id}")
async def status(ingestion_id: str):
    if ingestion_id not in ingestion_store:
        raise HTTPException(status_code=404, detail="Ingestion ID not found")

    try:
        ingestion = ingestion_store[ingestion_id]
        batch_statuses = [b["status"] for b in ingestion["batches"]]

        if all(status == BatchStatus.YET_TO_START for status in batch_statuses):
            overall_status = BatchStatus.YET_TO_START
        elif all(status == BatchStatus.COMPLETED for status in batch_statuses):
            overall_status = BatchStatus.COMPLETED
        else:
            overall_status = BatchStatus.TRIGGERED

        return {
            "ingestion_id": ingestion_id,
            "status": overall_status,
            "batches": [
                {"batch_id": b["batch_id"], "ids": b["ids"], "status": b["status"]}
                for b in ingestion["batches"]
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_batches():
    last_processed = 0
    while True:
        try:
            current_time = time.time()
            if current_time - last_processed < RATE_LIMIT_SECONDS:
                await asyncio.sleep(RATE_LIMIT_SECONDS - (current_time - last_processed))
            
            if not batch_queue:
                await asyncio.sleep(1)
                continue

            with lock:
                sorted_batches = sorted(list(batch_queue), key=lambda x: (x[0], x[1]))
                batch_queue.clear()
                batch_queue.extend(sorted_batches)

                priority, created_time, batch = batch_queue.popleft()

            # Process the batch
            ingestion_id = batch["ingestion_id"]
            batch["status"] = BatchStatus.TRIGGERED

            ingestion = ingestion_store[ingestion_id]
            for b in ingestion["batches"]:
                if b["batch_id"] == batch["batch_id"]:
                    b["status"] = BatchStatus.TRIGGERED
                    break

            # Simulate external API processing
            await asyncio.sleep(PROCESS_DELAY)
            response = {"id": "<id>", "data": "processed"}  # Mock response
            
            # Update batch status
            batch["status"] = BatchStatus.COMPLETED
            for b in ingestion["batches"]:
                if b["batch_id"] == batch["batch_id"]:
                    b["status"] = BatchStatus.COMPLETED
                    break

            last_processed = time.time()

        except Exception as e:
            print(f"Error in batch processing: {e}")
            await asyncio.sleep(1)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_batches())
