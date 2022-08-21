from pathlib import Path

from google.protobuf import text_format
from p4.config.v1.p4info_pb2 import P4Info


def read_p4info_txt(file_path: str) -> P4Info:
    """Read p4info.txt file."""
    p4_info = P4Info()
    text_format.Parse(Path(file_path).expanduser().read_text(), p4_info)
    return p4_info


def read_bytes_config(config_json_path: str) -> bytes:
    return Path(config_json_path).expanduser().read_bytes()
