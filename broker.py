#!/usr/bin/env python3
"""
EventStreamCore: Lightweight Asynchronous Pub/Sub Broker
Zero external dependencies. Pure Python asyncio.
"""

import asyncio
import json
import logging
from typing import Dict, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("EventStreamBroker")

class PubSubBroker:
    def __init__(self, host: str = "127.0.0.1", port: int = 8765):
        self.host = host
        self.port = port
        # Topic -> Set of active client writers
        self.topics: Dict[str, Set[asyncio.StreamWriter]] = {}
        # Topic -> Recent message history buffer (ring buffer style, max 50 items)
        self.history: Dict[str, list] = {}
        self.clients: Set[asyncio.StreamWriter] = set()

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        addr = server.sockets[0].getsockname()
        logger.info(f"EventStreamBroker active on {addr[0]}:{addr[1]}")
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info('peername')
        logger.info(f"New client connected: {peer}")
        self.clients.add(writer)

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                
                try:
                    packet = json.loads(line.decode().strip())
                except json.JSONDecodeError:
                    await self.send_response(writer, {"status": "error", "message": "Invalid JSON format"})
                    continue

                await self.process_command(writer, packet)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error handling client {peer}: {e}")
        finally:
            logger.info(f"Client disconnected: {peer}")
            self.clients.remove(writer)
            self._cleanup_client_subscriptions(writer)
            writer.close()
            await writer.wait_closed()

    async def process_command(self, writer: asyncio.StreamWriter, packet: dict):
        action = packet.get("action", "").upper()
        topic = packet.get("topic")

        if action == "PING":
            await self.send_response(writer, {"status": "ok", "message": "PONG"})

        elif action == "LIST":
            topics_info = {t: len(subs) for t, subs in self.topics.items()}
            await self.send_response(writer, {"status": "ok", "topics": topics_info})

        elif action == "SUBSCRIBE":
            if not topic:
                await self.send_response(writer, {"status": "error", "message": "Topic required for subscription"})
                return
            
            if topic not in self.topics:
                self.topics[topic] = set()
                self.history[topic] = []

            self.topics[topic].add(writer)
            await self.send_response(writer, {"status": "ok", "message": f"Subscribed to '{topic}'"})
            logger.info(f"Client subscribed to topic: {topic}")

            # Send last 5 historical messages if available
            if self.history[topic]:
                for past_msg in self.history[topic][-5:]:
                    await self.send_response(writer, {"event": "history", "topic": topic, "payload": past_msg})

        elif action == "UNSUBSCRIBE":
            if topic and topic in self.topics:
                self.topics[topic].discard(writer)
                await self.send_response(writer, {"status": "ok", "message": f"Unsubscribed from '{topic}'"})

        elif action == "PUBLISH":
            if not topic:
                await self.send_response(writer, {"status": "error", "message": "Topic required for publication"})
                return
            
            payload = packet.get("payload")
            if topic not in self.topics:
                self.topics[topic] = set()
                self.history[topic] = []

            # Store in history buffer (max 50)
            self.history[topic].append(payload)
            if len(self.history[topic]) > 50:
                self.history[topic].pop(0)

            await self.send_response(writer, {"status": "ok", "message": "Published", "recipients": len(self.topics[topic])})

            # Fan-out broadcast to all subscribers on this topic
            if self.topics[topic]:
                message_frame = json.dumps({
                    "event": "message",
                    "topic": topic,
                    "payload": payload
                }).encode() + b"\n"

                dead_writers = set()
                for sub_writer in self.topics[topic]:
                    try:
                        sub_writer.write(message_frame)
                        await sub_writer.drain()
                    except Exception:
                        dead_writers.add(sub_writer)

                # Clean up broken subscriber pipes
                for dead in dead_writers:
                    self.topics[topic].discard(dead)

        else:
            await self.send_response(writer, {"status": "error", "message": f"Unknown action: {action}"})

    async def send_response(self, writer: asyncio.StreamWriter, data: dict):
        writer.write(json.dumps(data).encode() + b"\n")
        await writer.drain()

    def _cleanup_client_subscriptions(self, writer: asyncio.StreamWriter):
        for topic in list(self.topics.keys()):
            self.topics[topic].discard(writer)
            if not self.topics[topic]:
                # Optional: clean up empty topics
                pass

if __name__ == "__main__":
    broker = PubSubBroker()
    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        logger.info("Broker stopped by user.")