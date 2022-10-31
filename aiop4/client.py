import asyncio
import logging
from typing import Iterable, Optional, Union

import grpc
import p4.v1.p4runtime_pb2 as p4r_pb2
import p4.v1.p4runtime_pb2_grpc as p4r_grpc
from grpc.aio import AioRpcError
from p4.config.v1 import p4info_pb2

from aiop4.utils import read_bytes_config, read_p4info_txt

from .elems_info import ElementsP4Info
from .exceptions import BecomePrimaryException

log = logging.getLogger(__name__)

match_type = Union[
    p4r_pb2.FieldMatch.Exact,
    p4r_pb2.FieldMatch.Ternary,
    p4r_pb2.FieldMatch.LPM,
    p4r_pb2.FieldMatch.Range,
    p4r_pb2.FieldMatch.Optional,
]


class Client:
    """asyncio P4Runtime Client."""

    def __init__(
        self,
        host="localhost:9559",
        device_id=0,
        election_id=p4r_pb2.Uint128(high=1, low=0),
    ) -> None:
        """asyncio P4Runtime Client."""
        self.host = host
        self.device_id = device_id
        self.election_id = election_id
        self.p4info: p4info_pb2.P4Info = None
        self.elems_info: ElementsP4Info = None

        self.queue: asyncio.Queue = asyncio.Queue()

        self._stream_channel: grpc.StreamStreamMultiCallable = None
        self._channel = grpc.aio.insecure_channel(self.host)
        self._stub = p4r_grpc.P4RuntimeStub(self._channel)
        self._is_primary = asyncio.Event()
        self._stream_control_task: asyncio.Task = None

    @property
    def host_device(self) -> str:
        return f"{self.host}:{self.device_id}"

    def is_primary(self) -> bool:
        """Check if this client is the primary controller."""
        if self._is_primary.is_set():
            return True
        return False

    @property
    def stream_channel(self) -> grpc.StreamStreamMultiCallable:
        """Client's stream control channel."""
        if not self._stream_channel:
            self._stream_channel = self._stub.StreamChannel()
        return self._stream_channel

    async def become_primary_or_raise(self, *, timeout=5) -> None:
        """Try to become the primary controller or raise asyncio.TimeoutError."""
        task = asyncio.create_task(self.try_to_become_primary())
        try:
            await asyncio.wait_for(self._is_primary.wait(), timeout)
        except asyncio.TimeoutError:
            if task.exception():
                raise asyncio.TimeoutError(str(task._exception))
            task.cancel()
            self._stream_channel = None
            raise

    async def get_capabilities(self) -> str:
        """GetCapabilities. Get P4Runtime API version implemented by the server."""
        return await self._stub.Capabilities(p4r_pb2.CapabilitiesRequest())

    async def get_fwd_pipeline(self) -> p4r_pb2.GetForwardingPipelineConfigResponse:
        """GetForwardingPipelineConfig."""
        return await self._stub.GetForwardingPipelineConfig(
            p4r_pb2.GetForwardingPipelineConfigRequest(device_id=self.device_id)
        )

    async def try_to_become_primary(self):
        """Try to become primary."""
        req = p4r_pb2.StreamMessageRequest(
            arbitration=p4r_pb2.MasterArbitrationUpdate(
                device_id=self.device_id, election_id=self.election_id
            )
        )
        try:
            await self.stream_channel.write(req)
        except AioRpcError as exc:
            log.error(f"{str(exc)} payload {req.__class__.__name__}: {req}")
            raise

        response = await self.stream_channel.read()
        which_update = response.WhichOneof("update")
        log.debug(f"Got message {which_update} from {self.host_device} {response}")
        if which_update == "arbitration":
            self._is_primary.set()
        else:
            raise BecomePrimaryException(f"Unexpected update type {response}")

        self._stream_control_task = asyncio.create_task(self.stream_control())

    async def stream_control(self):
        """stream_control."""
        try:
            log.info(f"Starting stream_control for {self.host_device}")
            response = await self.stream_channel.read()
            while response != grpc.aio.EOF:
                which_update = response.WhichOneof("update")
                log.debug(
                    f"Got message {which_update} from device {self.device_id} "
                    f"{self.host} {response}"
                )
                if which_update == "digest" or which_update == "packet":
                    await self.queue.put(response)
                elif which_update == "arbitration":
                    # TODO put on another internal queue
                    pass
                elif which_update == "idle_timeout_notification":
                    # TODO put on another internal queue
                    pass
                elif which_update == "error":
                    log.error(f"Got StreamError {response}")
                    await self.queue.put(response)
                elif which_update == "other":
                    pass
                else:
                    log.warning(f"Got unsupported update type {response}")
                response = await self.stream_channel.read()

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
            log.debug(f"Sending WriteRequest to {self.host_device} {req}")
            return await self._stub.Write(req)
        except AioRpcError as exc:
            log.error(f"{str(exc)} payload {req.__class__.__name__} {req}")
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

    async def ack_digest_list(self, digest: p4r_pb2.DigestList) -> None:
        """Ack DigestList."""
        req = p4r_pb2.StreamMessageRequest(
            digest_ack=p4r_pb2.DigestListAck(
                digest_id=digest.digest_id, list_id=digest.list_id
            )
        )
        try:
            await self.stream_channel.write(req)
        except AioRpcError as exc:
            log.error(f"{str(exc)} payload {req.__class__.__name__}: {req}")
            raise

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
        self.p4info = pipeline.config.p4info
        self.elems_info = ElementsP4Info(self.p4info)

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
        p4info, device_config = await asyncio.gather(
            loop.run_in_executor(None, read_p4info_txt, p4_info_txt_path),
            loop.run_in_executor(None, read_bytes_config, config_json_path),
        )
        return await self._set_fwd_pipeline(
            p4r_pb2.ForwardingPipelineConfig(
                p4info=p4info,
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

    async def modify_entity(self, *entities: p4r_pb2.Entity) -> None:
        """Modify entities."""
        return await self._op_entity(*entities, op_type=p4r_pb2.Update.Type.MODIFY)

    async def insert_entity(self, *entities: p4r_pb2.Entity) -> None:
        """Insert entities."""
        return await self._op_entity(*entities, op_type=p4r_pb2.Update.Type.INSERT)

    async def delete_entity(self, *entities: p4r_pb2.Entity) -> None:
        """Delete entities."""
        return await self._op_entity(*entities, op_type=p4r_pb2.Update.Type.DELETE)

    async def _op_entity(self, *entities, op_type: int):
        """Perform operations on entities."""
        try:
            payload = [
                p4r_pb2.Update(
                    type=op_type,
                    entity=entity,
                )
                for entity in entities
            ]
            return await self._write_request(*payload)
        except AioRpcError as exc:
            log.error(f"{str(exc)} payload {payload}")
            raise

    def new_table_entry(
        self,
        table: str,
        field_matches: dict[str, match_type],
        action: str,
        action_params: Optional[list[bytes]] = None,
        *,
        priority=0,
        idle_timeout_ns=0,
    ) -> p4r_pb2.Entity:
        """new_table_entry."""
        # TODO raise specific err
        action_params = action_params if action_params else []
        table = self.elems_info.tables[table]
        action = self.elems_info.actions[action]
        return p4r_pb2.Entity(
            table_entry=p4r_pb2.TableEntry(
                table_id=table.preamble.id,
                match=[
                    p4r_pb2.FieldMatch(
                        **{
                            "field_id": self.elems_info.table_match_fields[
                                (table.preamble.name, k)
                            ].id,
                            f"{v.__class__.__name__.lower()}": v,
                        }
                    )
                    for k, v in field_matches.items()
                ],
                action=p4r_pb2.TableAction(
                    action=p4r_pb2.Action(
                        action_id=action.preamble.id,
                        params=[
                            p4r_pb2.Action.Param(param_id=i, value=v)
                            for i, v in enumerate(action_params, 1)
                        ],
                    )
                ),
                priority=priority,
                idle_timeout_ns=idle_timeout_ns,
                is_default_action=False if field_matches else True,
            )
        )
