# Data Ingestion API System

A simple API system for submitting data ingestion requests and checking their status. The system processes data in batches asynchronously while respecting rate limits and priorities.

## Features

- RESTful API endpoints for data ingestion and status checking
- Batch processing with configurable size
- Priority-based processing (HIGH, MEDIUM, LOW)
- Rate limiting (1 batch per 5 seconds)
- Asynchronous processing
- In-memory storage for simplicity

## Setup and Running

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the server:
   ```bash
   uvicorn main:app --reload --port 5000
   ```

## API Endpoints

### 1. Ingestion API
- **Endpoint:** POST /ingest
- **Input:** JSON payload with IDs and priority
  ```json
  {
    "ids": [1, 2, 3, 4, 5],
    "priority": "HIGH"
  }
  ```
- **Output:** Ingestion ID
  ```json
  {
    "ingestion_id": "abc123"
  }
  ```

### 2. Status API
- **Endpoint:** GET /status/{ingestion_id}
- **Output:** Status and batch details
  ```json
  {
    "ingestion_id": "abc123",
    "status": "triggered",
    "batches": [
      {"batch_id": "uuid", "ids": [1, 2, 3], "status": "completed"},
      {"batch_id": "uuid", "ids": [4, 5], "status": "triggered"}
    ]
  }
  ```

## Technical Details

- Built with FastAPI
- Uses async/await for non-blocking operations
- In-memory storage using dictionaries and deque
- Simulated external API calls with delays
- Thread-safe operations using locks
