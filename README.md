# target-miso
This package is a [Singer](https://singer.io) target that sends data to Miso's [Data API](https://api.askmiso.com). Being a singer target, you can integrate it into your data pipeline using your favorite DataOps framework, for instance, [Meltano](https://meltano.com/).

# Use with Meltano

Follow the steps below to setup `target-miso` in your Meltano project. Or see the most essential project example [here](https://github.com/MisoAI/target-miso/tree/main/examples/basic).

## Setup
Setup `meltano.yml` like this:

```yml
# ...
plugins:
  # ...
  loaders:
  - name: target-miso
    namespace: target_miso
    pip_url: git+https://github.com/askmiso/target-miso.git@v0.9.1
    executable: target-miso
    config:
      template_folder: template
      api_server: https://api.askmiso.com
      api_key: your_miso_api_key
      use_async: false
```

Make sure to run `meltano install` to install the dependency.

## Configuration

The config object accepts the following properties:

| name | required | explanation |
| --- | --- | --- |
| `template_folder` | true | Where you keep the template files. The path is relative to Meltano project directory. |
| `api_server` | true | The Miso API server host. |
| `api_key` | true | Your Miso API key. |
| `use_async` | true | Whether to send request in asynchronous mode. |

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

### Customized filters

We created some customize filters to dealing our current customers' data. Please feel free to add or optimize the filter for these filters.
* datetime_format: We support three formats of date-time string.
    * %Y-%m-%d %H:%M:%S
    * %Y-%m-%dT%H:%M:%S+00:00
    * %Y-%m-%d
* list_of_str: In this filter, we support changing the string to a list of strings. Like , "apple" to ["apple"].
* convert_categories: In this filter, we support changing the string to a list that supports a list of strings. Like, "apple" to [["apple"]].
* remove_symbol: We support removing some symbols to an empty string in this filter. Like, double quote, backslash, \\N, “, \r\n, \n to an empty string.

----

Copyright &copy; 2021 Miso Corp.
