#!/usr/bin/env python3
import io
import simplejson as json
import sys
from pathlib import Path
from typing import Dict

import _jsonnet
import sentry_sdk
import singer
from sentry_sdk import set_tag

from .miso import MisoWriter

logger = singer.get_logger()


def emit_state(state):
    """ Output state to STDOUT"""
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def eval_jsonnet(snippet: str, data: dict):
    data_json = json.dumps(data)
    output = _jsonnet.evaluate_snippet(
        "snippet",
        f'local data = {data_json};\n' + snippet)
    return json.loads(output)


def persist_messages(messages, miso_client: MisoWriter, stream_to_template: Dict[str, str]):
    state = None
    schemas = {}
    for message in messages:
        try:
            msg_obj = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            raise ValueError(f"Unable to parse: {message}")
        message_type = msg_obj['type']
        if 'stream' in msg_obj and msg_obj['stream'] not in stream_to_template:
            raise ValueError(f"template for stream: {msg_obj['stream']} not found")
        if message_type == 'RECORD':
            # write a record to Miso
            steam_name = msg_obj['stream']
            template: str = stream_to_template[steam_name]
            miso_record = None
            try:
                miso_record = eval_jsonnet(template, msg_obj['record'])
            except Exception:
                logger.exception("Unable to parse record: %s", msg_obj['record'])
            if miso_record:
                miso_client.write_record(miso_record)
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(msg_obj['value']))
            state = msg_obj['value']
        elif message_type == 'SCHEMA':
            stream = msg_obj['stream']
            schemas[stream] = msg_obj['schema']
        else:
            logger.warning("Unknown message type {} in message {}".format(msg_obj['type'], msg_obj))
    # write remain records in the buffer
    miso_client.flush()

    # # Start delete removed data
    # if len(product_ids) > 0:
    #     delete_datasets(product_ids, 'products')
    # if len(user_ids) > 0:
    #     delete_datasets(user_ids, 'users')
    return state


# def delete_datasets(ids: list, data_type: str):
#     del_ids = set(get_miso_ids(data_type)).difference(set(ids))
#     if len(del_ids) > 0:
#         bulk_delete_product(del_ids, data_type)


def main():
    params = singer.utils.parse_args({'template_folder', 'api_server', 'api_key'})

    # load Miso API parameters
    api_server = params.config['api_server']
    api_key = params.config['api_key']
    miso_client = MisoWriter(api_server, api_key)

    if 'sentry_dsn' in params.config:
        sentry_sdk.init(dsn=params.config['sentry_dsn'])
        if 'sentry_source' in params.config:
            set_tag("source", params.config['sentry_source'])

    # load templates
    template_folder_path = Path(params.config['template_folder'])
    if not template_folder_path.exists():
        raise ValueError(f"template_folder {params.config['template_folder']} does not exist")
    stream_to_template = {path.stem: path.open().read() for path in template_folder_path.glob('*.jsonnet')}

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(input_messages, miso_client, stream_to_template)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
