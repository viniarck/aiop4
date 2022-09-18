import asyncio
from unittest.mock import AsyncMock

import grpc
import p4.v1.p4runtime_pb2 as p4r_pb2
import pytest


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


async def test_become_primary_or_raise(client) -> None:
    """Test become_primary_or_raise."""
    client.try_to_become_primary = AsyncMock()
    client._is_primary.set()
    await client.become_primary_or_raise()
    assert client.is_primary()


async def test_become_primary_or_raise_timeout(client) -> None:
    """Test become_primary_or_raise timeout."""
    client.try_to_become_primary = AsyncMock()
    client._is_primary = AsyncMock()
    client._is_primary.wait.side_effect = asyncio.TimeoutError
    with pytest.raises(asyncio.TimeoutError):
        await client.become_primary_or_raise()
    assert client._stream_channel is None


async def test_stream_control(client) -> None:
    """Test stream_control."""
    client._stream_channel.read.side_effect = [
        p4r_pb2.StreamMessageResponse(arbitration=p4r_pb2.MasterArbitrationUpdate()),
        grpc.aio.EOF,
    ]
    await client.stream_control()
    assert client.stream_channel.read.call_count == 2


async def test_enable_digest(client) -> None:
    """Test enable_diges."""
    _id = 10
    await client.enable_digest(_id)
    assert client._stub.Write.call_count == 1
    arg = client._stub.Write.call_args[0][0]
    assert isinstance(arg, p4r_pb2.WriteRequest)
    assert arg.updates[0].entity.digest_entry.digest_id == _id


def test_new_table_entry(client, elems_info) -> None:
    """Test new_table_entry."""
    client.elems_info = elems_info
    priority = 100
    idle_timeout_ns = 1000000000
    entity = client.new_table_entry(
        "IngressImpl.dmac",
        {},
        "IngressImpl.fwd",
        [b"\x01"],
        priority=priority,
        idle_timeout_ns=idle_timeout_ns,
    )
    assert entity.table_entry.table_id == 45595255
    assert entity.table_entry.action.action.action_id == 19387472
    assert entity.table_entry.priority == priority
    assert entity.table_entry.idle_timeout_ns == idle_timeout_ns
    assert entity.table_entry.action.action.params[0].value == b"\x01"


async def test_set_fwd_pipeline(client):
    """test set_fwd_pipeline."""

    assert not client.p4info
    assert not client.elems_info
    await client._set_fwd_pipeline(p4r_pb2.ForwardingPipelineConfig())
    assert client._stub.SetForwardingPipelineConfig.call_count == 1
    assert client._stub.GetForwardingPipelineConfig.call_count == 1
    assert client.p4info
    assert client.elems_info


async def test_ack_digest_list(client):
    """Test ack_digest_list."""
    digest_id, list_id = 1, 2
    digest_list = p4r_pb2.DigestList(digest_id=digest_id, list_id=list_id)
    await client.ack_digest_list(digest_list)
    assert client._stream_channel.write.call_count == 1
    arg = client._stream_channel.write.call_args[0][0].digest_ack
    assert arg.digest_id == digest_id
    assert arg.list_id == list_id


async def test_op_entity(client):
    """Test op_entity."""
    entity, op_type = p4r_pb2.Entity(), p4r_pb2.Update.Type.MODIFY
    await client._op_entity(entity, op_type=op_type)
    assert client._stub.Write.call_count == 1
    arg = client._stub.Write.call_args[0][0]
    assert arg.updates[0].entity == entity
    assert arg.updates[0].type == op_type


async def test_modify_entity(client):
    """Test modify_entity."""
    entity = p4r_pb2.Entity()
    client._op_entity = AsyncMock()
    await client.modify_entity(entity)
    client._op_entity.assert_called_with(entity, op_type=p4r_pb2.Update.Type.MODIFY)


async def test_insert_entity(client):
    """Test insert_entity."""
    entity = p4r_pb2.Entity()
    client._op_entity = AsyncMock()
    await client.insert_entity(entity)
    client._op_entity.assert_called_with(entity, op_type=p4r_pb2.Update.Type.INSERT)


async def test_delete_entity(client):
    """Test delete_entity."""
    entity = p4r_pb2.Entity()
    client._op_entity = AsyncMock()
    await client.delete_entity(entity)
    client._op_entity.assert_called_with(entity, op_type=p4r_pb2.Update.Type.DELETE)


def test_host_device_str(client):
    """Test host_device str."""
    assert client.host_device == f"{client.host}:{client.device_id}"
