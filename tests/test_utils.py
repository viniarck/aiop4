from unittest.mock import patch

from aiop4.utils import read_bytes_config, read_p4info_txt


@patch("aiop4.utils.Path")
@patch("aiop4.utils.text_format")
def test_read_p4info_txt(text_format, path) -> None:
    """Test read_p4info_txt."""
    assert read_p4info_txt("some_path")
    assert text_format.Parse.call_count == 1
    assert path.call_count == 1


@patch("aiop4.utils.Path")
def test_read_bytes_config(path) -> None:
    """Test read_bytes_config."""
    assert read_bytes_config("some_json_path")
    assert path.call_count == 1
