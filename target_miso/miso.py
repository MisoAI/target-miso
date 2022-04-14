#!/usr/bin/env python3
from typing import List, Dict, Set

import requests
import singer
from requests import HTTPError
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = singer.get_logger()


def check_miso_data_type(record):
    """ Determine data type """
    if 'product_id' in record:
        data_type = 'products'
    elif 'type' in record and ('user_id' in record or 'anonymous_id' in record):
        data_type = 'interactions'
    elif 'user_id' in record:
        data_type = 'users'
    else:
        raise ValueError(f'This record is not product, user, nor interaction: {record}')
    return data_type


class MisoWriter:
    def __init__(self, api_server: str, api_key: str, use_async: bool):
        self.type_to_buffer = {'products': [], 'interactions': [], 'users': []}

        retry_strategy = Retry(
            total=3,
            status_forcelist=[401, 429, 500, 502, 503, 504, 403],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session: requests.Session = requests.Session()
        self.session.mount("https://", adapter)
        self.api_server = api_server
        self.api_key = api_key
        self.use_async = use_async

    def _send_request(self, data: List[Dict], data_type: str):
        logger.info("try to send %s requests to %s-data-api, async:%s.",
                    len(data), data_type, self.use_async)
        try:
            response = self.session.post(
                '{}/v1/{}?api_key={}&async={}'.format(self.api_server, data_type, self.api_key,
                                                      1 if self.use_async else 0),
                json={'data': list(data)}
            )
            response.raise_for_status()
            logger.info(response.text)
        except HTTPError as error:
            logger.exception("Request %s\n Response %s", data, error.response.text)
        except ConnectionError:
            logger.exception('Connection error')

    def get_existing_ids(self, data_type: str):
        """ Get existing ids from Miso _ids API """
        logger.info("try to get %s ids from Miso.", data_type)
        try:
            res = self.session.get(
                '{}/v1/{}/_ids?api_key={}'.format(self.api_server, data_type, self.api_key)
            )
            res.raise_for_status()
            return res.json()['data']['ids']
        except HTTPError as err:
            if err.response.status_code == 404:
                return []
            raise

    def delete_records(self, bulk_del_ids: Set[str], data_type: str):
        """ Delete a list of records from Miso """
        logger.info("Send bulk delete %s by ids to miso. Ids: %s", data_type, bulk_del_ids)
        col_name = 'product_ids'
        if data_type == 'users':
            col_name = 'user_ids'
        ret = self.session.post(
            '{}/v1/{}/_delete?api_key={}'.format(self.api_server, data_type, self.api_key),
            json={"data": {col_name: list(bulk_del_ids)}})
        logger.info("Receive response from delete %s", data_type)
        ret.raise_for_status()

    def write_record(self, record: Dict):
        """ Write record to Miso """
        data_type = check_miso_data_type(record)
        buffer = self.type_to_buffer[data_type]
        buffer.append(record)
        limit = 1000 if data_type == 'interactions' else 200
        if len(buffer) >= limit:
            self._send_request(buffer, data_type)
            buffer.clear()

    def flush(self):
        for data_type in list(self.type_to_buffer):
            buf = self.type_to_buffer[data_type]
            if buf:
                self._send_request(buf, data_type)
                buf.clear()
