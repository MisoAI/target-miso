""" test persist messages function """
import json
from pathlib import Path
from unittest.mock import MagicMock

from target_miso.extensions import get_jinja_env
from target_miso.py_extensions import import_code
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
        {"test_stream": template},
        {},
        {}
    )
    dummy_client.write_record.assert_called_once_with(
        {
            "product_id": "123",
            "title": 'title 123'
        }
    )
    dummy_client.flush.assert_called_once_with()


template_path = Path(__file__).parent.joinpath('templates')


def test_persist_message_jinja():
    """ Test persist message function is working as expected """
    dummy_client = MagicMock()
    dummy_client.write_record = MagicMock()
    dummy_client.flush = MagicMock()
    raw_rec = {"user_id": 123, "product_id": "title 123",
               "timestamp": "2022-03-26T18:45:53+00:00"}
    jinja_env = get_jinja_env(template_path)

    persist_messages(
        [json.dumps({"type": "RECORD", "stream": "test_stream", "record": raw_rec})],
        dummy_client,
        {},
        {'test_stream': jinja_env.get_template('interaction.jinja')},
        {}
    )
    dummy_client.write_record.assert_called_once_with(
        {'user_id': '123', 'type': 'product_detail_page_view',
         'timestamp': '2022-03-26T18:45:53+00:00',
         'product_ids': ['title 123']})
    dummy_client.flush.assert_called_once_with()


def test_persist_message_py():
    """ Test persist message function is working as expected """
    dummy_client = MagicMock()
    dummy_client.write_record = MagicMock()
    dummy_client.flush = MagicMock()
    raw_rec = {"user_id": 123, "product_id": "title 123",
               "timestamp": "2022-03-26T18:45:53+00:00"}
    pyfn = import_code(
"""
def transform(x):
    return {'user_id': str(x['user_id']),
            'product_ids': [x["product_id"]],
            'timestamp': x["timestamp"],
            'type': 'product_detail_page_view'
            }
""", 'test')
    persist_messages(
        [json.dumps({"type": "RECORD", "stream": "test_stream", "record": raw_rec})],
        dummy_client,
        {},
        {},
        {'test_stream': pyfn}
    )
    dummy_client.write_record.assert_called_once_with(
        {'user_id': '123',
         'product_ids': ['title 123'],
         'timestamp': '2022-03-26T18:45:53+00:00',
         'type': 'product_detail_page_view'
         })
    dummy_client.flush.assert_called_once_with()
