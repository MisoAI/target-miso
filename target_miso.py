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

import requests
import singer
from jinja2 import Environment, FileSystemLoader
from jsonschema import Draft4Validator
from requests import HTTPError
from requests.adapters import HTTPAdapter
from singer import utils
from urllib3 import Retry


params = utils.parse_args({'template_folder', 'api_server', 'api_key'})
folder_path = params.config['template_folder']
api_server = params.config['api_server']
api_key = params.config['api_key']


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
    value = value.replace('"', '')
    value = value.replace("\\", '')
    value = value.replace("\\\\N", '')
    value = value.replace("“", '')
    value = value.replace("\r\n", '')
    value = value.replace("\n", '')
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
        logger.info(data)
        logger.info(error.response.text)
        logger.exception(error)
    except ConnectionError as error:
        logger.info(data)
        logger.exception(error)


def persist_messages(messages, env: Environment):
    state = None
    data = []
    schemas = key_properties = validators = {}
    data_type = None
    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if 'stream' in o and not path.exists(f'{folder_path}/{o["stream"]}.json'):
            logger.error('Unknown stream {} in message.'.format(o['stream']))
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
            if not data_type:
                if 'product_id' in result:
                    data_type = 'products'
                elif 'user_id' in result:
                    if 'type' in result:
                        data_type = 'interactions'
                    else:
                        data_type = 'users'
                else:
                    data_type = 'interactions'
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
        if len(data) > 100:
            send_request(data, data_type)
            data = []
    if len(data) > 0:
        send_request(data, data_type)
    return state


def main():
    if not os.path.isdir(folder_path):
        logger.exception(f'{folder_path} not found.')
        raise
    env = Environment(loader=FileSystemLoader(folder_path), trim_blocks=True)
    env.filters['datetime_format'] = datetime_format
    env.filters['list_of_str'] = str_to_list_of_str
    env.filters['convert_categories'] = str_to_categories
    env.filters['remove_symbol'] = remove_symbol

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(input_messages, env)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
