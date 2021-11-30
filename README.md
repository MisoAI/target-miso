A [Singer](https://singer.io) target that writes data to Miso [data-api](https://api.askmiso.com).

## How to use it

The `target-miso` works together with any other [Singer Tap] to move data from sources like [Braintree](https://github.com/singer-io/tap-braintree), [Freshdesk](https://github.com/singer-io/tap-freshdesk), and [Hubspot](https://github.com/singer-io/tap-hubspot) to Miso data-api. First, you'll need to create jinja2 templates in the templates folder. The template file name should be `{stream_name}.json`, and the sample content could be down below.

# target-jsonl

This repo is a [Singer](https://singer.io) target that writes data to Miso [data-api](https://api.askmiso.com).

## How to use it

The `target-miso` works together with any other [Singer Tap](https://www.singer.io) to move data from sources like [Braintree](https://github.com/singer-io/tap-braintree), [Freshdesk](https://github.com/singer-io/tap-freshdesk), and [Hubspot](https://github.com/singer-io/tap-hubspot) to Miso data-api. First, you'll need to create jinja2 templates in the templates folder. The template file name should be `{stream_name}.json`, and the sample content could be down below.

## Customize filter for jinja2

We created some customize filters to dealing our current customers' data. Please feel free to add or optimize the filter for these filters.
* datetime_format: We support three formats of date-time string.
    * %Y-%m-%d %H:%M:%S
    * %Y-%m-%dT%H:%M:%S+00:00
    * %Y-%m-%d
* list_of_str: In this filter, we support changing the string to a list of strings. Like , "apple" to ["apple"].
* convert_categories: In this filter, we support changing the string to a list that supports a list of strings. Like, "apple" to [["apple"]].
* remove_symbol: We support removing some symbols to an empty string in this filter. Like, double quote, backslash, \\N, “, \r\n, \n to an empty string.

### Sample jinja2 template file

```json
{
    "user_id": "{{ data.sso }}",
    "type": "share",
    "timestamp": "{{ data.share_date|datetime_format }}",
    "product_ids": {{ data.nid|list_of_str }},
    "context": {
        "custom_context": {
            "verb": "{{ data.verb }}",
            "asset_id": "{{ data.asset_id }}",
            "asset_title": "{{ data.asset_titla }}",
            "asset_url": "{{ data.asset_url }}",
            "domain_name": "{{ data.domain_name }}",
            "tag": "{{ data.tag }}",
            "tag_description": "{{ data.tag_description }}",
            "instructor_sso": "{{ data.instructor_sso }}",
            "shared_method": "{{ data.shared_method }}"
        }
    }
}
```

## Sample config file.

```json
{
    "template_folder": "/template",
    "api_server": "https://api.askmiso.com",
    "api_key": "xxx"
}
```

---

Copyright &copy; 2021 Miso Corp.
