from unittest.mock import AsyncMock

import pytest

from aiop4.client import Client


@pytest.fixture
def client() -> Client:
    """aoip4 Client."""
    client = Client()
    client._stub = AsyncMock()
    client._stream_channel = AsyncMock()
    client._channel = AsyncMock()
    return client
