"""
Drop your real A6 AI call in here. Everything else in this project
(queue, worker, retries, status endpoint) doesn't care what happens
inside this function — only that it's async and may raise.
"""
import asyncio
import random


async def call_ai(payload: dict) -> dict:
    # TODO: replace this body with your actual A6 call, e.g.:
    #   response = await anthropic_client.messages.create(...)
    #   return {"summary": response.content[0].text}
    await asyncio.sleep(3)  # simulates the slow network call

    if random.random() < 0.3:  # simulates a transient failure (timeout, 5xx, etc.)
        raise RuntimeError("simulated transient AI provider failure")

    return {"summary": f"AI processed payload: {payload}"}
