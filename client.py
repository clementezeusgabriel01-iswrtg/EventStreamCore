#!/usr/bin/env python3
"""
Test Client for EventStreamCore Broker
"""

import asyncio
import json
import sys

async def client_listener(reader):
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            data = json.loads(line.decode().strip())
            print(f"\n[INCOMING EVENT] -> {json.dumps(data, indent=2)}\n> ", end="")
    except asyncio.CancelledError:
        pass

async def main():
    reader, writer = await asyncio.open_connection('127.0.0.1', 8765)
    print("Connected to EventStreamCore Broker.")
    print("Commands:")
    print('  {"action": "SUBSCRIBE", "topic": "news"}')
    print('  {"action": "PUBLISH", "topic": "news", "payload": {"text": "Hello World!"}}')
    print('  {"action": "LIST"}')
    
    listener_task = asyncio.create_task(client_listener(reader))

    try:
        while True:
            prompt_input = await asyncio.get_event_loop().run_in_executor(None, input, "> ")
            if not prompt_input.strip():
                continue
            try:
                # Validate JSON before sending
                parsed = json.loads(prompt_input)
                writer.write((json.dumps(parsed) + "\n").encode())
                await writer.drain()
            except json.JSONDecodeError:
                print("Error: Input must be valid JSON.")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        listener_task.cancel()
        writer.close()
        await writer.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())