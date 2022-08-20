from unittest.mock import AsyncMock

import grpc
import p4.v1.p4runtime_pb2 as p4r_pb2


async def test_get_capabilities(client) -> None:
    """Test get_capabilities."""
    await client.get_capabilities()
    assert client._stub.Capabilities.call_count == 1
    client._stub.Capabilities.assert_called_with(p4r_pb2.CapabilitiesRequest())


async def test_get_fwd_pipeline(client) -> None:
    """Test get_fwd_pipeline."""
    await client.get_fwd_pipeline()
    assert client._stub.GetForwardingPipelineConfig.call_count == 1
    arg = p4r_pb2.GetForwardingPipelineConfigRequest(device_id=client.device_id)
    client._stub.GetForwardingPipelineConfig.assert_called_with(arg)


def test_is_primary(client) -> None:
    """Test is_primary."""
    assert not client.is_primary()
    client._is_primary.set()
    assert client.is_primary()


async def test_try_to_become_primary(client) -> None:
    """Test try_to_become_primary."""
    client._stream_channel.read.return_value = p4r_pb2.StreamMessageResponse(
        arbitration=p4r_pb2.MasterArbitrationUpdate()
    )
    client.stream_control = AsyncMock()
    await client.try_to_become_primary()
    assert client.stream_control.call_count == 1
    assert client.stream_channel.read.call_count == 1
    assert client._stream_control_task
    assert client.is_primary()


async def test_stream_control(client) -> None:
    """Test stream_control."""
    client._stream_channel.read.side_effect = [
        p4r_pb2.StreamMessageResponse(arbitration=p4r_pb2.MasterArbitrationUpdate()),
        grpc.aio.EOF,
    ]
    await client.stream_control()
    assert client.stream_channel.read.call_count == 2
