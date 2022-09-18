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


async def learn_mac(client: Client, digest: p4r_pb2.DigestList):
    entities = []
    for item in digest.data:
        src_addr = item.struct.members[0].bitstring
        in_port = item.struct.members[1].bitstring
        smac_entry = client.new_table_entry(
            "IngressImpl.smac",
            {"hdr.ethernet.srcAddr": p4r_pb2.FieldMatch.Exact(value=src_addr)},
            "NoAction",
        )
        dmac_entry = client.new_table_entry(
            "IngressImpl.dmac",
            {"hdr.ethernet.dstAddr": p4r_pb2.FieldMatch.Exact(value=src_addr)},
            "IngressImpl.fwd",
            [in_port],
        )
        entities.extend([smac_entry, dmac_entry])

    await client.insert_entity(*entities)
    await client.ack_digest_list(digest)


async def consumer(client: Client, queue: asyncio.Queue):
    while True:
        msg = await queue.get()
        log.debug(f"Consumer device_id {client.device_id} got message {msg}")
        match msg.WhichOneof("update"):
            case "digest":
                asyncio.create_task(learn_mac(client, msg.digest))
            case "error":
                log.error(f"Got StreamError {msg}")
            case _:
                pass


async def l2_client(
    host: str, device_id: int, p4_info_txt_path: str, config_json_path: str
):
    """l2_client."""

    client = Client(host=host, device_id=device_id)
    tasks = set()
    log.info(f"Starting Client {client.host_device}")
    log.info(await client.get_capabilities())

    await client.become_primary_or_raise(timeout=5)
    task = asyncio.create_task(consumer(client, client.queue))
    tasks.add(task)
    await client.set_fwd_pipeline_from_file(p4_info_txt_path, config_json_path)
    await client.enable_digest(client.elems_info.digests["digest_t"].preamble.id)

    ports, mgrp = [0, 1, 2, 3, 4, 5, 6, 7], 0xAB
    await client.insert_multicast_group(mgrp, ports)
    table_entry = client.new_table_entry(
        "IngressImpl.dmac", {}, "IngressImpl.broadcast", [struct.pack("!h", mgrp)]
    )
    await client.modify_entity(table_entry)


async def main():
    """main."""
    p4_info_txt_path = os.getenv(
        "P4INFO_FILE", "~/repos/aiop4/examples/l2_switch/l2_switch.p4info.txt"
    )
    config_json_path = os.getenv(
        "P4CONFIG_JSON", "~/repos/aiop4/examples/l2_switch/l2_switch.json"
    )
    assert os.path.isfile(
        os.path.expanduser(p4_info_txt_path)
    ), f"{p4_info_txt_path} isn't a file"
    assert os.path.isfile(
        os.path.expanduser(config_json_path)
    ), f"{config_json_path} isn't a file"

    client1 = asyncio.create_task(
        l2_client(
            host="localhost:9559",
            device_id=1,
            p4_info_txt_path=p4_info_txt_path,
            config_json_path=config_json_path,
        )
    )
    client2 = asyncio.create_task(
        l2_client(
            host="localhost:9560",
            device_id=2,
            p4_info_txt_path=p4_info_txt_path,
            config_json_path=config_json_path,
        )
    )
    clients = [client1, client2]
    await asyncio.gather(*clients)
    input("Type any key to stop ")


if __name__ == "__main__":
    asyncio.run(main())
