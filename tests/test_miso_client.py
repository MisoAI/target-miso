from unittest.mock import MagicMock

from target_miso.miso import MisoWriter


def test_write_and_flush():
    """ Test write and flush """
    client = MisoWriter(api_server='https://test.com', api_key='secret', use_async=False)
    client.session = MagicMock()
    client.session.post = MagicMock()
    product = {'product_id': 'test'}
    for _ in range(200):
        client.write_record(product)
    client.session.post.assert_called_once_with(
        'https://test.com/v1/products?api_key=secret', json={'data': [product] * 200}
    )
    client.session.post = MagicMock()
    user = {'user_id': 'test'}
    for _ in range(200):
        client.write_record(user)
    client.session.post.assert_called_once_with(
        'https://test.com/v1/users?api_key=secret', json={'data': [user] * 200}
    )

    client.session.post = MagicMock()
    interaction = {'user_id': 'test', 'type': 'product_detail_page_view'}
    for _ in range(1000):
        client.write_record(interaction)
    client.session.post.assert_called_once_with(
        'https://test.com/v1/interactions?api_key=secret', json={'data': [interaction] * 1000}
    )

    client.session.post = MagicMock()
    client.write_record(interaction)
    client.write_record(product)
    client.write_record(user)
    client.flush()
    client.session.post.assert_any_call(
        'https://test.com/v1/interactions?api_key=secret', json={'data': [interaction]}
    )
    client.session.post.assert_any_call(
        'https://test.com/v1/products?api_key=secret', json={'data': [product]}
    )
    client.session.post.assert_any_call(
        'https://test.com/v1/users?api_key=secret', json={'data': [user]}
    )

