import asyncio
import logging
import os
import struct

import p4.v1.p4runtime_pb2 as p4r_pb2

from aiop4 import Client

log_format = (
    "%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d]"
    " (%(threadName)s) %(message)s"
)
logging.basicConfig(level=logging.DEBUG, format=log_format)
log = logging.getLogger(__name__)


class L2SWClient:
    """L2SWClient."""

    def __init__(
        self,
        client: Client,
        p4info_path: str,
        config_json_path: str,
        multicast_group=0xAB,
        ports=None,
    ) -> None:
        """L2SWClient."""
        self.client = client
        self.p4info_path = p4info_path
        self.config_json_path = config_json_path
        self.multicast_group = multicast_group
        self.ports = ports if ports else list(range(0, 7))
        self.consumer_task: asyncio.Task = None
        self.keep_consuming = True

    async def learn_mac(self, digest: p4r_pb2.DigestList):
        """learn_mac digest task."""
        entities = []
        for item in digest.data:
            src_addr = item.struct.members[0].bitstring
            in_port = item.struct.members[1].bitstring
            smac_entry = self.client.new_table_entry(
                "IngressImpl.smac",
                {"hdr.ethernet.srcAddr": p4r_pb2.FieldMatch.Exact(value=src_addr)},
                "NoAction",
            )
            dmac_entry = self.client.new_table_entry(
                "IngressImpl.dmac",
                {"hdr.ethernet.dstAddr": p4r_pb2.FieldMatch.Exact(value=src_addr)},
                "IngressImpl.fwd",
                [in_port],
            )
            entities.extend([smac_entry, dmac_entry])

        await self.client.insert_entity(*entities)
        await self.client.ack_digest_list(digest)

    async def digests_consumer(self) -> None:
        """digests consumer."""
        while self.keep_consuming:
            msg = await self.client.queue.get()
            log.debug(f"Consumer device_id {self.client.device_id} got message {msg}")
            which_msg = msg.WhichOneof("update")
            if which_msg == "digest":
                asyncio.create_task(self.learn_mac(msg.digest))
            elif which_msg == "error":
                log.error(f"Got StreamError {msg}")
            else:
                pass

    async def setup_config(self) -> None:
        """Setup config."""
        log.info(f"Setting up config for {self.client.host_device}")
        await self.client.become_primary_or_raise(timeout=5)
        self.consumer_task = asyncio.create_task(self.digests_consumer())
        await self.client.set_fwd_pipeline_from_file(
            self.p4info_path, self.config_json_path
        )
        await self.client.enable_digest(
            self.client.elems_info.digests["digest_t"].preamble.id
        )
        await self.client.insert_multicast_group(self.multicast_group, self.ports)
        table_entry = self.client.new_table_entry(
            "IngressImpl.dmac",
            {},
            "IngressImpl.broadcast",
            [struct.pack("!h", self.multicast_group)],
        )
        await self.client.modify_entity(table_entry)


async def main():
    """main entry point for linear_topo.py that has two switches."""
    p4info_path = os.getenv(
        "P4INFO_FILE", "~/repos/aiop4/examples/l2_switch/l2_switch.p4info.txt"
    )
    config_json_path = os.getenv(
        "P4CONFIG_JSON", "~/repos/aiop4/examples/l2_switch/l2_switch.json"
    )
    assert os.path.isfile(os.path.expanduser(p4info_path)), p4info_path
    assert os.path.isfile(os.path.expanduser(config_json_path)), config_json_path

    client1 = L2SWClient(Client("localhost:9559", 1), p4info_path, config_json_path)
    client2 = L2SWClient(Client("localhost:9560", 2), p4info_path, config_json_path)
    await asyncio.gather(*[client1.setup_config(), client2.setup_config()])
    await asyncio.gather(*[client1.digests_consumer(), client2.digests_consumer()])


if __name__ == "__main__":
    try:
        print("Hit <C-c> to stop this app")
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
