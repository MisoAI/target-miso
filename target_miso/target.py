#!/usr/bin/env python3
import datetime
import io

import pytz
import simplejson as json
import sys
from pathlib import Path
from typing import Dict, Callable

import _jsonnet
import sentry_sdk
import singer
from jinja2 import Template
from sentry_sdk import set_tag

from target_miso.extensions import get_jinja_env
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


def timestamp_to_str(dt: datetime.datetime):
    """ convert a datetime to string """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    dt = dt.replace(microsecond=0)
    return dt.isoformat()


def persist_messages(messages, miso_client: MisoWriter,
                     stream_to_template_jsonnet: Dict[str, str],
                     stream_to_template_jinja: Dict[str, Template],
                     stream_to_python_func: Dict[str, Callable]):
    state = None
    schemas = {}
    for message in messages:
        try:
            msg_obj = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            raise ValueError(f"Unable to parse: {message}")
        message_type = msg_obj['type']
        if 'stream' in msg_obj and (msg_obj['stream'] not in stream_to_template_jsonnet and
                                    msg_obj['stream'] not in stream_to_template_jinja and
                                    msg_obj['stream'] not in stream_to_python_func
        ):
            raise ValueError(f"template for stream: {msg_obj['stream']} not found")
        if message_type == 'RECORD':
            # write a record to Miso
            steam_name = msg_obj['stream']
            miso_record = None
            if steam_name in stream_to_template_jsonnet:
                template: str = stream_to_template_jsonnet[steam_name]
                try:
                    miso_record = eval_jsonnet(template, msg_obj['record'])
                except Exception:
                    logger.exception("Unable to parse record: %s", msg_obj['record'])
            if steam_name in stream_to_template_jinja:
                jinja_template: Template = stream_to_template_jinja[steam_name]
                try:
                    miso_record = json.loads(jinja_template.render(data=msg_obj['record']))
                except Exception:
                    logger.exception("Unable to parse record: %s", msg_obj['record'])
            if steam_name in stream_to_python_func:
                try:
                    miso_record = stream_to_python_func[steam_name](msg_obj['record'])
                except Exception:
                    logger.exception("Unable to parse record: %s", msg_obj['record'])

            if miso_record:
                check_timestamp_fields = ['updated_at', 'created_at', 'timestamp']
                for field in check_timestamp_fields:
                    if isinstance(miso_record.get(field), datetime.datetime):
                        miso_record[field] = timestamp_to_str(miso_record[field])

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
def import_code(code, name) -> Callable:
    """ code can be any object containing code -- string, file object, or
       compiled code object. Returns a new module object initialized
       by dynamically importing the given code and optionally adds it
       to sys.modules under the given name.
    """
    import imp
    try:
        module = imp.new_module(name)
        exec(code, module.__dict__)
    except:
        logger.exception('Failed to load code from %s', name)
        raise
    if 'transform' not in module.__dict__:
        raise ValueError('There is no transform function in the code')
    return module.transform


def import_code_path(path: Path) -> Callable:
    """ import code in a file """
    name = path.stem.replace('-', '_').replace('/', '_').replace('.', '_')
    return import_code(path.open().read(), name)


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
    stream_to_template_jsonnet = {
        path.stem: path.open().read() for path in
        template_folder_path.glob('*.jsonnet')}
    jinja_env = get_jinja_env(template_folder_path)
    stream_to_template_jinja: Dict[str, Template] = {
        path.stem: jinja_env.get_template(path.name) for path in
        template_folder_path.glob('*.jinja')}
    stream_to_python_func: Dict[str, Callable] = {
        path.stem: import_code_path(path)
        for path in
        template_folder_path.glob('*.py')}

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(input_messages, miso_client,
                             stream_to_template_jsonnet,
                             stream_to_template_jinja,
                             stream_to_python_func)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
