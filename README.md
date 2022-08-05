# target-miso
This package is a [Singer](https://singer.io) target that sends data to Miso's [Data API](https://api.askmiso.com). Being a singer target, you can integrate it into your data pipeline using your favorite DataOps framework, for instance, [Meltano](https://meltano.com/).

<p>
  <a href="https://pypi.org/project/target-miso/"><img alt="PyPI" src="https://img.shields.io/pypi/v/target-miso"></a>
  <img alt="PyPI - Python Version: 3" src="https://img.shields.io/pypi/pyversions/target-miso">
  <a href="https://github.com/MisoAI/target-miso/tree/main/LICENSE"><img alt="PyPI - License: MIT" src="https://img.shields.io/pypi/l/target-miso"></a>
</p>

# Use with Meltano

Follow the steps below to setup `target-miso` in your Meltano project. Or see the most essential project example [here](https://github.com/MisoAI/target-miso/tree/main/examples/csv).

## Setup
Setup `meltano.yml` like this:

```yml
# ...
plugins:
  # ...
  loaders:
  - name: target-miso
    namespace: target_miso
    pip_url: target-miso
    executable: target-miso
    config:
      template_folder: template
      api_key: your_miso_api_key
```

Make sure to run `meltano install` to install the dependency.

## Configuration

The config object accepts the following properties:

| name | required | default | explanation |
| --- | --- | --- |
| api_server | no | https://api.askmiso.com | The Miso API server host. |
| api_key | yes | | Your Miso API key. |
| template_folder | yes | | Where you keep the template files. The path is relative to Meltano project directory. |
| use_async | no | False | Whether to send request in asynchronous mode. |
| dry_run | no | False | Whether to send request in dry-run mode. |

## Replication methods

Currently, this target supports `FULL_TABLE` and `INCREMENTAL` replication methods. `LOG_BASED` is not yet supported.

## Template

To specify how records are transformed into payloads of Miso API, for each stream from the tap, put a corresponding [jinja2](https://jinja.palletsprojects.com/en/3.1.x/) template file in your template folder. For example, given a stream `product`, put a template file `product.jinja` like this:

```nunjucks
{
  "product_id": "{{ data.uuid }}",
  "created_at": "{{ data.created_at | datetime_format }}",
  "title": "{{ data.title }}",
  "description": "{{ data.description }}",
  "categories": "{{ data.category | convert_categories }}",
  "custom_attributes": {
    "vender": "{{ data.vender if data.vender }}"
  }
}
```

### Rules on output data types

Miso takes 3 kinds of data records: `user`, `product`, and `interaction`. A record is classified into one of these type by the following rules:
* If the payload contains the `type` field, it is an interaction record.
* If the payload contains the `user_id` field, it is a user record.
* If the payload contains the `product_id` field, it is a product record.

### Built-in filters

Target Miso comes with a few built-in filters that can be used in template expressions:

#### `datetime_format`

Takes a string in any format compatible with [dateparser](https://dateparser.readthedocs.io/en/latest/) and output in ISO format, which is desired by Miso API.

#### `list_of_str`

Wrap a string to a singleton list of string. For example, `"apple"` to `["apple"]`.

#### `convert_categories`

Wrap a string to a singleton double-layered list of string. For example, `"apple"` to `[["apple"]]`.

#### `remove_symbol`

1. Convert an int to string.
1. Strip off some special characters from input string, including: `"`, `\`, `\\N`, `“`, `\r\n`, `\n`, `\r`.

#### `split`

Split a string into a list by comma.

#### `fix_url`

Encode (as URL component) the path component of a URL string.

#### `jsonify`

Serialize an obj to a JSON string.

----

Copyright &copy; 2021 Miso Corp.
