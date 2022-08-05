#!/usr/bin/env python3
import datetime
import hashlib
import io
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Callable, Set, Optional

import _jsonnet
import pytz
import sentry_sdk
import simplejson as json
import singer
from jinja2 import Template
from sentry_sdk import set_tag

from target_miso.extensions import get_jinja_env
from target_miso.py_extensions import import_code_path
from .miso import MisoWriter, check_miso_data_type

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


# stream to the seen product_ids or user_ids
stream_to_ids: Dict[str, Set] = defaultdict(set)
# stream to data type
stream_to_datatype: Dict[str, str] = {}

MISO_STATE_KEY = '__miso_target_state__'


def update_state(miso_upload_state: Dict, stream_name: str, record_id: str, record: Optional[Dict]):
    """ Remember what we uploaded """
    if not miso_upload_state:
        miso_upload_state = {}
    if stream_name not in miso_upload_state:
        miso_upload_state[stream_name] = {}
    if record:
        record_hash = hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()
        miso_upload_state[stream_name][record_id] = record_hash
    else:
        if record_id in miso_upload_state[stream_name]:
            del miso_upload_state[stream_name][record_id]
    return miso_upload_state


def is_upload_needed(miso_upload_state: Dict, stream_name: str, record_id: str, record: Dict):
    """ Whether we need to upload a record to Miso """
    if miso_upload_state is None:
        return True
    record_hash = hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()
    return miso_upload_state.get(stream_name, {}).get(record_id) != record_hash


def persist_messages(messages,
                     miso_client: MisoWriter,
                     stream_to_template_jsonnet: Dict[str, str],
                     stream_to_template_jinja: Dict[str, Template],
                     stream_to_python_func: Dict[str, Callable],
                     extra_config):
    state = {}
    miso_upload_state = {}
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
            stream_name = msg_obj['stream']
            miso_record = None
            if stream_name in stream_to_template_jsonnet:
                template: str = stream_to_template_jsonnet[stream_name]
                try:
                    miso_record = eval_jsonnet(template, msg_obj['record'])
                except Exception:
                    logger.exception("Unable to parse record: %s", msg_obj['record'])
            if stream_name in stream_to_template_jinja:
                jinja_template: Template = stream_to_template_jinja[stream_name]
                try:
                    miso_record = json.loads(jinja_template.render(data=msg_obj['record']))
                except Exception:
                    logger.exception("Unable to parse record: %s", msg_obj['record'])
            if stream_name in stream_to_python_func:
                try:
                    miso_record = stream_to_python_func[stream_name](msg_obj['record'])
                except Exception:
                    logger.exception("Unable to parse record: %s", msg_obj['record'])

            if miso_record:
                check_timestamp_fields = ['updated_at', 'created_at', 'timestamp']
                for field in check_timestamp_fields:
                    if isinstance(miso_record.get(field), datetime.datetime):
                        miso_record[field] = timestamp_to_str(miso_record[field])
                stream_to_datatype[stream_name] = check_miso_data_type(miso_record)
                if stream_to_datatype[stream_name] != 'interactions':
                    record_id = miso_record.get('product_id') or miso_record.get('user_id')
                    # maintain the ids we have seen
                    stream_to_ids[stream_name].add(record_id)
                    # whether we need to upload this record
                    if is_upload_needed(miso_upload_state, stream_name, record_id, miso_record):
                        miso_client.write_record(miso_record)
                        #
                        if stream_to_datatype[stream_name] == 'products':
                            miso_upload_state = update_state(
                                miso_upload_state, stream_name, record_id, record=miso_record
                            )
                else:
                    # write interaction directly
                    miso_client.write_record(miso_record)


        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(msg_obj['value']))
            state = msg_obj['value']
            if not miso_upload_state:
                # update miso_upload_state
                miso_upload_state = state.get(MISO_STATE_KEY, {})
        elif message_type == 'SCHEMA':
            stream = msg_obj['stream']
            schemas[stream] = msg_obj['schema']
        elif message_type == 'ACTIVATE_VERSION':
            logger.warning('ACTIVATE_VERSION %s', msg_obj)
            stream_name = msg_obj['stream']
            data_type = stream_to_datatype.get(stream_name)
            shall_delete = not extra_config['insert_only']
            if shall_delete and stream_to_ids.get(stream_name) and data_type in ('users', 'products'):
                logger.warning('Perform ids check %s:%s', stream_name, msg_obj['version'])
                existing_ids: Set[str] = set(miso_client.get_existing_ids(data_type))
                to_delete_ids = existing_ids - stream_to_ids[stream_name]
                if to_delete_ids:
                    logger.warning('Delete %s %s: %s', len(to_delete_ids), stream_name, to_delete_ids)
                    miso_client.delete_records(to_delete_ids, data_type)
                    for record_id in to_delete_ids:
                        # maintain state
                        miso_upload_state = update_state(miso_upload_state, stream_name, record_id, None)
                else:
                    logger.warning('No need to delete anything from Miso for %s', stream_name)
                del stream_to_ids[stream_name]
                del stream_to_datatype[stream_name]
            else:
                logger.warning("Ignore ACTIVATE_VERSION %s", msg_obj)
        else:
            logger.warning("Unknown message type {} in message {}".format(msg_obj['type'], msg_obj))
    # write remain records in the buffer
    miso_client.flush()
    state[MISO_STATE_KEY] = miso_upload_state
    return state

def is_truthy(value):
    return (str(value).lower() in ('true', '1')) if value != None else False

def main():
    params = singer.utils.parse_args({'template_folder', 'api_key'})

    # load Miso API parameters
    api_server = params.config.get('api_server') or 'https://api.askmiso.com'
    api_key = params.config['api_key']
    use_async = is_truthy(params.config.get('use_async'))
    dry_run = is_truthy(params.config.get('dry_run'))

    miso_client = MisoWriter(api_server, api_key, use_async, dry_run)
    extra_config = {
        'insert_only': is_truthy(params.config.get('insert_only'))
    }

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
    state = persist_messages(input_messages,
                             miso_client,
                             stream_to_template_jsonnet,
                             stream_to_template_jinja,
                             stream_to_python_func,
                             extra_config)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
