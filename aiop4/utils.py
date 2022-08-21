from pathlib import Path
from typing import Optional

import p4.v1.p4runtime_pb2 as p4r_pb2
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


def find_match_field(p4_info: P4Info, table: str, name: str) -> p4r_pb2.FieldMatch:
    """Find match field."""
    table = find_by_preamble_attr(p4_info, "tables", table)
    res = next(
        (match_field for match_field in table.match_fields if match_field.name == name),
        None,
    )
    if res:
        return res
    raise ValueError(f"MatchField {name} not found in {table}")


def read_p4_info_txt(file_path: str) -> P4Info:
    """Read p4_info.txt file."""
    p4_info = P4Info()
    text_format.Parse(Path(file_path).expanduser().read_text(), p4_info)
    return p4_info


def read_bytes_config(config_json_path: str) -> bytes:
    return Path(config_json_path).expanduser().read_bytes()
