#!/usr/bin/env python3

from pathlib import Path
from typing import Callable

import singer

logger = singer.get_logger()


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
