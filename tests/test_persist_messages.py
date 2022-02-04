""" test persist messages function """
import json
from unittest.mock import MagicMock

from target_miso.target import persist_messages

def test_persist_message():
    """ Test persist message function is working as expected """
    dummy_client = MagicMock()
    dummy_client.write_record = MagicMock()
    dummy_client.flush = MagicMock()
    raw_rec = {"asset_id": 123, "asset_title": "title 123"}
    template = """
        {
            # convert asset_id to product_id string
            product_id: std.toString(data.asset_id),
            # convert asset_title to simply title 
            title: data.asset_title
        }
    """
    persist_messages(
        [json.dumps({"type": "RECORD", "stream": "test_stream", "record": raw_rec})],
        dummy_client,
        {"test_stream": template}
    )
    dummy_client.write_record.assert_called_once_with(
        {
            "product_id": "123",
            "title": 'title 123'
        }
    )
    dummy_client.flush.assert_called_once_with()