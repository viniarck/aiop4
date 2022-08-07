import asyncio
import logging
from typing import Iterable

import grpc
import p4.v1.p4runtime_pb2 as p4r_pb2
import p4.v1.p4runtime_pb2_grpc as p4r_grpc
from grpc.aio import AioRpcError
from p4.config.v1 import p4info_pb2

from aiop4.p4_info import find_by_preamble_attr, read_bytes_config, read_p4_info_txt

log = logging.getLogger(__name__)


class Client:

    """asyncio P4Runtime Client."""

    def __init__(
        self,
        host="localhost:9559",
        device_id=0,
        election_id=p4r_pb2.Uint128(high=1, low=0),
    ) -> None:
        """Client."""
        self.host = host
        self.device_id = device_id
        self.election_id = election_id
        self.p4_info: p4info_pb2.P4Info = None
        self._channel = grpc.aio.insecure_channel(self.host)
        self._stub = p4r_grpc.P4RuntimeStub(self._channel)

    async def get_capabilities(self) -> str:
        """GetCapabilities. Get P4Runtime API version implemented by the server."""
        return await self._stub.Capabilities(p4r_pb2.CapabilitiesRequest())

    async def get_fwd_pipeline(self) -> p4r_pb2.GetForwardingPipelineConfigResponse:
        """GetForwardingPipelineConfig."""
        return await self._stub.GetForwardingPipelineConfig(
            p4r_pb2.GetForwardingPipelineConfigRequest(device_id=self.device_id)
        )

    async def stream_control(self, event: asyncio.Event):
        """stream_control."""
        # TODO handle this better
        streaming = self._stub.StreamChannel()
        req = p4r_pb2.StreamMessageRequest(
            arbitration=p4r_pb2.MasterArbitrationUpdate(
                device_id=self.device_id, election_id=self.election_id
            )
        )
        try:
            await streaming.write(req)
        except AioRpcError as exc:
            log.error(f"{str(exc)} payload {req.__class__.__name__}: {req}")
            raise

        try:
            response = None
            log.info(f"Started stream_control for device_id {self.device_id}")
            while response != grpc.aio.EOF:
                response = await streaming.read()
                which_update = response.WhichOneof("update")
                log.debug(
                    f"Got message {which_update} from device {self.device_id} "
                    f"{self.host} {response}"
                )
                match which_update:
                    case "digest":
                        pass
                    case "arbitration":
                        event.set()
                    case "packet":
                        pass
                    case "idle_timeout_notification":
                        pass
                    case "error":
                        log.error(f"Got StreamError {response}")
                    case "other":
                        pass
                    case _:
                        log.warning(f"Got unsupported update type {response}")

        except AioRpcError as e:
            log.warning(f"AioRpcError {str(e)} {e.code()}")

    async def _write_request(
        self,
        *updates: Iterable[p4r_pb2.Update],
        atomicity=p4r_pb2.WriteRequest.Atomicity.CONTINUE_ON_ERROR,
    ) -> None:
        """_write_request."""
        req = p4r_pb2.WriteRequest(
            device_id=self.device_id,
            election_id=self.election_id,
            updates=updates,
            atomicity=atomicity,
        )
        try:
            return await self._stub.Write(req)
        except AioRpcError as exc:
            log.error(f"{str(exc)} payload {req.__class__.__name__}: {req}")
            raise

    async def enable_digest(self, _id: int) -> None:
        """write_update."""
        update = p4r_pb2.Update(
            type=p4r_pb2.Update.Type.INSERT,
            entity=p4r_pb2.Entity(
                digest_entry=p4r_pb2.DigestEntry(
                    digest_id=_id,
                    config=p4r_pb2.DigestEntry.Config(
                        max_timeout_ns=0, max_list_size=1, ack_timeout_ns=1000000000
                    ),
                )
            ),
        )
        return await self._write_request(update)

    async def _set_fwd_pipeline(
        self,
        config: p4r_pb2.ForwardingPipelineConfig,
        action=p4r_pb2.SetForwardingPipelineConfigRequest.Action.VERIFY_AND_COMMIT,
    ) -> p4r_pb2.GetForwardingPipelineConfigResponse:
        """_set_fwd_pipeline."""
        req = p4r_pb2.SetForwardingPipelineConfigRequest(
            device_id=self.device_id,
            election_id=self.election_id,
            action=action,
            config=config,
        )
        try:
            response = await self._stub.SetForwardingPipelineConfig(req)
        except AioRpcError as exc:
            log.error(f"{str(exc)} payload {req.__class__.__name__}: {req}")
            raise

        pipeline = await self.get_fwd_pipeline()
        self.p4_info = pipeline.config.p4info

        return response

    async def set_fwd_pipeline_from_file(
        self,
        p4_info_txt_path: str,
        config_json_path: str,
        cookie=0,
        action=p4r_pb2.SetForwardingPipelineConfigRequest.Action.VERIFY_AND_COMMIT,
    ) -> p4r_pb2.GetForwardingPipelineConfigResponse:
        """set_forwarding_pipeline_config."""

        loop = asyncio.get_running_loop()
        p4_info, device_config = await asyncio.gather(
            loop.run_in_executor(None, read_p4_info_txt, p4_info_txt_path),
            loop.run_in_executor(None, read_bytes_config, config_json_path),
        )
        return await self._set_fwd_pipeline(
            p4r_pb2.ForwardingPipelineConfig(
                p4info=p4_info,
                p4_device_config=device_config,
                cookie=p4r_pb2.ForwardingPipelineConfig.Cookie(cookie=cookie),
            )
        )

    async def insert_multicast_group(self, mgid: int, ports: list[int]):
        """insert_multicast_group."""
        update = p4r_pb2.Update(
            type=p4r_pb2.Update.Type.INSERT,
            entity=p4r_pb2.Entity(
                packet_replication_engine_entry=p4r_pb2.PacketReplicationEngineEntry(
                    multicast_group_entry=p4r_pb2.MulticastGroupEntry(
                        multicast_group_id=mgid,
                        replicas=[
                            p4r_pb2.Replica(egress_port=v, instance=i)
                            for i, v in enumerate(ports)
                        ],
                    )
                )
            ),
        )
        return await self._write_request(update)

    async def new_table_entry(
        self, table: str, action: str, action_params, is_default_action=False
    ):
        """new_table_entry."""
        table = find_by_preamble_attr(self.p4_info, "tables", table)
        log.info(f"table {table}")
        action = find_by_preamble_attr(self.p4_info, "actions", action)
        log.info(f"action {action}")

        update = p4r_pb2.Update(
            type=p4r_pb2.Update.Type.MODIFY,
            entity=p4r_pb2.Entity(
                table_entry=p4r_pb2.TableEntry(
                    table_id=table.preamble.id,
                    action=p4r_pb2.TableAction(
                        action=p4r_pb2.Action(
                            action_id=action.preamble.id,
                            params=[
                                p4r_pb2.Action.Param(param_id=i, value=v)
                                for i, v in enumerate(action_params, 1)
                            ],
                        )
                    ),
                    is_default_action=is_default_action,
                )
            ),
        )
        return await self._write_request(update)
