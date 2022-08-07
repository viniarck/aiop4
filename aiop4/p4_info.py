from typing import Optional

from google.protobuf import text_format
from p4.config.v1.p4info_pb2 import (
    Action,
    ActionProfile,
    ControllerPacketMetadata,
    Counter,
    Digest,
    DirectCounter,
    DirectMeter,
    Extern,
    Meter,
    P4Info,
    Register,
    Table,
    ValueSet,
)

P4InfoObject = (
    Table
    | Action
    | ActionProfile
    | Counter
    | DirectCounter
    | Meter
    | DirectMeter
    | ControllerPacketMetadata
    | ValueSet
    | Register
    | Digest
    | Extern
)

# TODO what about pre-index all of this by preamble id?


def get_by_preamble_attr(
    p4_info: P4Info, iter_name: str, value: str, preamble_attr="name", default=None
) -> Optional[P4InfoObject]:
    """Get element of a P4Info iterable by its preamble attr and value."""
    try:
        iterable = getattr(p4_info, iter_name)
        return next(
            (
                item
                for item in iterable
                if getattr(item.preamble, preamble_attr) == value
            ),
            default,
        )
    except (TypeError, AttributeError):
        return None


def find_by_preamble_attr(
    p4_info: P4Info, iter_name: str, value: str, preamble_attr="name"
) -> P4InfoObject:
    """Find element of a P4Info iterable by its preamble attr and value."""
    if item := get_by_preamble_attr(p4_info, iter_name, value, preamble_attr, None):
        return item
    raise ValueError(
        f"Preamble attr {preamble_attr} value {value} not found on {iter_name}"
    )


def read_p4_info_txt(path: str) -> P4Info:
    """Read p4_info.txt file."""
    p4_info = P4Info()
    with open(path, "r") as f:
        content = f.read()
        text_format.Parse(content, p4_info)
    return p4_info


def read_bytes_config(config_json_path: str) -> bytes:
    with open(config_json_path, "rb") as f:
        bytes_config = f.read()
        return bytes_config
