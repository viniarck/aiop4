from unittest.mock import AsyncMock

import pytest
from google.protobuf import text_format
from p4.config.v1.p4info_pb2 import P4Info

from aiop4.client import Client
from aiop4.elems_info import ElementsP4Info

from .data import p4info_data


@pytest.fixture
def client() -> Client:
    """aoip4 Client."""
    client = Client()
    client._stub = AsyncMock()
    client._stream_channel = AsyncMock()
    client._channel = AsyncMock()
    return client


@pytest.fixture
def p4info() -> P4Info:
    """P4Info."""
    p4_info = P4Info()
    text_format.Parse(p4info_data(), p4_info)
    return p4_info


@pytest.fixture
def elems_info(p4info) -> ElementsP4Info:
    """docstring."""
    return ElementsP4Info(p4info)
