import os

import httpx
import pytest

API_URL = os.environ.get("API_URL", "http://localhost:8000")
SINK_URL = os.environ.get("SINK_URL", "http://localhost:8080")
RABBITMQ_API = os.environ.get("RABBITMQ_API", "http://localhost:15672")
API_KEY = os.environ.get("API_KEY", "dev-secret-key")


@pytest.fixture
async def api():
    async with httpx.AsyncClient(base_url=API_URL, headers={"X-API-Key": API_KEY}, timeout=10) as c:
        yield c


@pytest.fixture
async def sink():
    async with httpx.AsyncClient(base_url=SINK_URL, timeout=10) as c:
        yield c
