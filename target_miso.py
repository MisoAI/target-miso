#!/usr/bin/env python3
import ast
import io
import json
import os
import sys
from datetime import datetime
from decimal import Decimal
from os import path
from typing import Callable, List, Dict, Optional
from urllib.parse import urlparse, quote

import requests
import sentry_sdk
import singer
from jinja2 import Environment, FileSystemLoader
from jsonschema import Draft4Validator
from requests import HTTPError
from requests.adapters import HTTPAdapter
from sentry_sdk import set_tag, capture_message, capture_exception
from singer import utils
from urllib3 import Retry


params = utils.parse_args({'template_folder', 'api_server', 'api_key'})
folder_path = params.config['template_folder']
api_server = params.config['api_server']
api_key = params.config['api_key']
if 'sentry_dsn' in params.config:
    sentry_sdk.init(dsn=params.config['sentry_dsn'])
    if 'sentry_source' in params.config:
        set_tag("source", params.config['sentry_source'])


def fix_url(value: str) -> str:
    url = urlparse(value)
    return url._replace(path=quote(url.path)).geturl()


def datetime_format(value: str) -> str:
    def parse_time(pattern: str) -> str:
        return datetime.utcfromtimestamp(
            datetime.strptime(value, pattern).timestamp()
        ).isoformat()

    try:
        return parse_time('%Y-%m-%d %H:%M:%S')
    except Exception:
        pass

    try:
        return parse_time('%Y-%m-%dT%H:%M:%S+00:00')
    except Exception:
        pass

    return parse_time('%Y-%m-%d')


def str_to_categories(value: Optional[str]) -> Optional[list]:
    if not value:
        return
    if value == 'NULL':
        return
    return [[str(value)]]


def str_split_with_comma(value: Optional[str]) -> list:
    if not value:
        return []
    return str(value).split(',')


def str_to_list_of_str(value: Optional[str]) -> Optional[list]:
    if not value:
        return
    if value == 'NULL':
        return
    return [str(value)]


def remove_symbol(value: Optional[str]) -> str:
    if not value:
        return ''
    if isinstance(value, int):
        return str(value)
    value = str(value).replace('"', '')
    value = value.replace("\\", '')
    value = value.replace("\\\\N", '')
    value = value.replace("“", '')
    value = value.replace("\r\n", '')
    value = value.replace("\n", '')
    value = value.replace(r'\r', '')
    return value


def with_retry(session_builder: Callable) -> Callable:
    """Return a HTTP session object with retry"""
    retry_strategy = Retry(
        total=3,
        status_forcelist=[401, 429, 500, 502, 503, 504, 403],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        backoff_factor=1,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    def wrapper(*args, **kwargs):
        builder = session_builder(*args, **kwargs)
        builder.mount("http://", adapter)
        builder.mount("https://", adapter)
        return builder

    return wrapper


logger = singer.get_logger()
session: requests.Session = with_retry(requests.Session)()


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def send_request(data: List[Dict], data_type: str):
    logger.info("try to send {} reuqest to data-api.".format(len(data)))
    try:
        response = session.post(
            '{}/v1/{}?api_key={}'.format(api_server, data_type, api_key),
            json={'data': data}
        )
        response.raise_for_status()
        logger.info(response.text)
    except HTTPError as error:
        capture_message("{}\n{}".format(error.response.text, data))
    except ConnectionError as error:
        capture_message("{}\n{}".format(error, data))


def get_miso_ids(data_type: str):
    logger.info("try to get %s miso ids.", data_type)
    try:
        res = session.get(
            '{}/v1/{}/_ids'.format(api_server, data_type),
            headers={'X-API-Key': api_key})
        res.raise_for_status()
        return res.json()['data']['ids']
    except HTTPError as err:
        if err.response.status_code == 404:
            return []
        logger.error(err)
        capture_exception(err)
    except ConnectionError as err:
        logger.error(err)
        capture_exception(err)


def bulk_delete_product(bulk_del_ids: set, data_type: str):
    logger.info("Send bulk delete %s by ids to miso", data_type)
    col_name = 'product_ids'
    if data_type == 'users':
        col_name = 'user_ids'
    for id in list(bulk_del_ids):
        logger.info("Tye to delete ID: %s", id)
    ret = session.post(
        '{}/v1/{}/_delete'.format(api_server, data_type),
        json={"data": {col_name: list(bulk_del_ids)}},
        headers={'X-API-Key': api_key})
    logger.info("Receive response from delete %s", data_type)
    ret.raise_for_status()


def persist_messages(messages, env: Environment):
    state = None
    data = []
    product_ids = []
    user_ids = []
    schemas = key_properties = validators = {}
    data_type = None
    limit = 99
    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            msg = "Unable to parse:\n{}".format(message)
            logger.error(msg)
            capture_message(msg)
            raise
        message_type = o['type']
        if 'stream' in o and not path.exists(f'{folder_path}/{o["stream"]}.json'):
            msg = 'Unknown stream {} in message.'.format(o['stream'])
            logger.error(msg)
            capture_message(msg)
            raise
        if message_type == 'RECORD':
            template = env.get_template(f"{o['stream']}.json")
            try:
                result = ast.literal_eval(template.render(data=o['record']))
                data.append(result)
            except Exception:
                msg = "Unable to parse record:\n{}".format(o['record'])
                logger.error(msg)
                capture_message(msg)
                continue

            # detect data_type everytime
            if 'product_id' in result:
                data_type = 'products'
            elif 'user_id' in result:
                if 'type' in result:
                    data_type = 'interactions'
                else:
                    data_type = 'users'
            else:
                data_type = 'interactions'

            # set limit, default use 100 for each bulk request
            limit = 99
            if data_type == 'interactions':
                limit = 999

            if data_type == 'products' and 'product_id' in result:
                product_ids.append(result['product_id'])
            elif data_type == 'users' and 'user_id' in result:
                user_ids.append(result['user_id'])
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = float_to_decimal(o['schema'])
            validators[stream] = Draft4Validator(float_to_decimal(o['schema']))
            key_properties[stream] = o['key_properties']
        else:
            logger.warning("Unknown message type {} in message {}".format(o['type'], o))
        if len(data) > limit:
            send_request(data, data_type)
            data = []
    if len(data) > 0:
        send_request(data, data_type)

    # Start delete removed data
    if len(product_ids) > 0:
        delete_datasets(product_ids, 'products')
    if len(user_ids) > 0:
        delete_datasets(user_ids, 'users')
    return state


def delete_datasets(ids: list, data_type: str):
    del_ids = set(get_miso_ids(data_type)).difference(set(ids))
    if len(del_ids) > 0:
        bulk_delete_product(del_ids, data_type)


def main():
    if not os.path.isdir(folder_path):
        logger.exception(f'{folder_path} not found.')
        raise
    env = Environment(loader=FileSystemLoader(folder_path), trim_blocks=True)
    env.filters['datetime_format'] = datetime_format
    env.filters['list_of_str'] = str_to_list_of_str
    env.filters['convert_categories'] = str_to_categories
    env.filters['remove_symbol'] = remove_symbol
    env.filters['split'] = str_split_with_comma
    env.filters['fix_url'] = fix_url

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(input_messages, env)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
