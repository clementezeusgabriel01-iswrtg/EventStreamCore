# EventStreamCore ⚡

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](https://docs.python.org/3/library/asyncio.html)

**EventStreamCore** is a lightweight, zero-dependency, ultra-fast asynchronous Publish/Subscribe (Pub/Sub) event broker and message streamer engineered entirely in pure Python using standard library `asyncio` and custom TCP socket framing.

Designed for hackathons, microservice event buses, lightweight telemetry pipelines, and real-time streaming applications, **EventStreamCore** provides high throughput and fan-out capabilities without the deployment overhead of heavy enterprise tools like Apache Kafka or RabbitMQ.

---

## 📸 Overview & Logo

<p align="center">
  <img src="logo.jpg" alt="EventStreamCore Logo" width="600" />
</p>

---

## 🌟 Key Features

* **Zero External Dependencies:** Built 100% on pure Python standard libraries (`asyncio`, `json`, `logging`).
* **Asynchronous Non-Blocking Core:** Handles thousands of concurrent TCP socket connections via Python's single-threaded event loop.
* **Line-Framed Protocol:** Uses robust newline-delimited JSON (`\n`) for clean TCP message framing and payload extraction.
* **Topic-Based Routing:** Efficient dynamic topic registration and real-time fan-out message distribution.
* **In-Memory Ring Buffer History:** Caches up to 50 recent events per topic, allowing new subscribers to catch up automatically upon connection.
* **Automatic Dead-Pipe Cleanup:** Detects abrupt client disconnects during broadcast cycles and purges stale handles instantly.
* **Developer Friendly:** Includes built-in interactive CLI client scripts for quick testing and prototyping.

---

## 🏗 System Architecture

```
                       ┌─────────────────────────────────────┐
                       │          EventStreamCore            │
                       │               Broker                │
                       └──────────────────┬──────────────────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
                    ▼                     ▼                     ▼
             ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
             │ Topic Registry│     │ History Ring│       │ Command     │
             │   & Fan-Out │       │   Buffers   │       │ Router      │
             └──────┬──────┘       └──────┬──────┘       └──────┬──────┘
                    │                     │                     │
                    └─────────────────────┼─────────────────────┘
                                          │
                                   TCP / Line JSON
                                          │
           ┌──────────────────────────────┼──────────────────────────────┐
           │                              │                              │
           ▼                              ▼                              ▼
  ┌─────────────────┐            ┌─────────────────┐            ┌─────────────────┐
  │ Publisher A     │            │ Subscriber B    │            │ Subscriber C    │
  │ (Metrics Pipe)  │            │ (Log Analyzer)  │            │ (Alert Monitor) │
  └─────────────────┘            └─────────────────┘            └─────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
* **Python 3.8+** (No third-party packages required).

### Installation
Clone the repository:
```bash
git clone https.github.com/yourusername/eventstreamcore.git
cd eventstreamcore
```

### Running the Broker
Start the standalone async server on port `8765`:
```bash
python3 broker.py
```

*Output:*
```text
2026-10-29 18:00:00 [INFO] EventStreamBroker active on 127.0.0.1:8765
```

### Running the Test Client
Open one or more terminal tabs and run the interactive testing client:
```bash
python3 client.py
```

---

## 📡 Protocol & Specification

All communication between clients and the **EventStreamCore** broker occurs over a standard TCP connection using **newline-delimited JSON** frames.

### Message Frame Structure
Each frame sent to or received from the broker **must terminate with a `\n` (newline)**.

```
+------------------------------------------------------+------+
|                  JSON Payload                        |  \n  |
+------------------------------------------------------+------+
```

---

### Command API Reference

#### 1. `SUBSCRIBE`
Registers the active connection to receive live broadcasts for a given topic.

* **Request:**
  ```json
  {"action": "SUBSCRIBE", "topic": "system_alerts"}
  ```
* **Response:**
  ```json
  {"status": "ok", "message": "Subscribed to 'system_alerts'"}
  ```
* **Replay Event (If history exists):**
  ```json
  {
    "event": "history",
    "topic": "system_alerts",
    "payload": {"level": "CRITICAL", "node": "us-east-1"}
  }
  ```

---

#### 2. `PUBLISH`
Broadcasts a payload to all active subscribers on a topic and stores it in the topic's ring buffer.

* **Request:**
  ```json
  {
    "action": "PUBLISH",
    "topic": "system_alerts",
    "payload": {
      "level": "WARNING",
      "msg": "CPU utilization above 90%"
    }
  }
  ```
* **Response to Publisher:**
  ```json
  {"status": "ok", "message": "Published", "recipients": 3}
  ```
* **Broadcast Received by Subscribers:**
  ```json
  {
    "event": "message",
    "topic": "system_alerts",
    "payload": {
      "level": "WARNING",
      "msg": "CPU utilization above 90%"
    }
  }
  ```

---

#### 3. `LIST`
Queries active topics and their subscriber counts.

* **Request:**
  ```json
  {"action": "LIST"}
  ```
* **Response:**
  ```json
  {
    "status": "ok",
    "topics": {
      "system_alerts": 3,
      "user_signups": 12
    }
  }
  ```

---

#### 4. `UNSUBSCRIBE`
Removes the current client handle from a topic's distribution list.

* **Request:**
  ```json
  {"action": "UNSUBSCRIBE", "topic": "system_alerts"}
  ```
* **Response:**
  ```json
  {"status": "ok", "message": "Unsubscribed from 'system_alerts'"}
  ```

---

#### 5. `PING`
Health check endpoint.

* **Request:**
  ```json
  {"action": "PING"}
  ```
* **Response:**
  ```json
  {"status": "ok", "message": "PONG"}
  ```

---

## 🐍 Programmatic Python Usage

You can easily integrate **EventStreamCore** directly into your Python apps using raw `asyncio` sockets:

```python
import asyncio
import json

async def run_publisher():
    reader, writer = await asyncio.open_connection("127.0.0.1", 8765)

    # Publish an event
    payload = {
        "action": "PUBLISH",
        "topic": "telemetry",
        "payload": {"temp": 23.5, "humidity": 45}
    }
    
    writer.write((json.dumps(payload) + "\n").encode())
    await writer.drain()

    # Read server confirmation
    response = await reader.readline()
    print("Broker Response:", json.loads(response.decode().strip()))

    writer.close()
    await writer.wait_closed()

asyncio.run(run_publisher())
```

---

## 🛠 Hackathon & Production Extensions

Looking to extend **EventStreamCore** during a hackathon or for custom projects? Here are recommended modules to add:

1. **Regex Wildcard Topics:** Modify topic lookup to support pattern matching (e.g., `logs.*` or `sensor/+/temp`).
2. **Persistence Layer:** Dump message history buffers to SQLite or an append-only WAL file on disk.
3. **Authentication Layer:** Require an `AUTH <token>` initial packet before processing further commands.
4. **WebSocket Bridge:** Add a lightweight HTTP/WebSocket bridge layer to allow browser clients to subscribe natively.

---
