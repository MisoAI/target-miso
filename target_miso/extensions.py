import json
from typing import Optional
from urllib.parse import urlparse, quote

import dateparser
import pytz
from jinja2 import Environment, FileSystemLoader


def fix_url(value: str) -> str:
    url = urlparse(value)
    return url._replace(path=quote(url.path)).geturl()


def datetime_format(value: str) -> str:
    dt = dateparser.parse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    dt = dt.replace(microsecond=0)
    return dt.isoformat()


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


def get_jinja_env(folder_path):
    env = Environment(loader=FileSystemLoader(folder_path), trim_blocks=True)
    env.filters['datetime_format'] = datetime_format
    env.filters['list_of_str'] = str_to_list_of_str
    env.filters['convert_categories'] = str_to_categories
    env.filters['remove_symbol'] = remove_symbol
    env.filters['split'] = str_split_with_comma
    env.filters['fix_url'] = fix_url
    env.filters['jsonify'] = json.dumps
    return env
