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
