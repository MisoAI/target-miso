""" Test eval jsonnet """
from target_miso.target import eval_jsonnet


def test_transform():
    """ Test jsonnet transformation """
    raw_rec = {"asset_id": 123, "asset_title": "title 123"}

    # basic transform
    output = eval_jsonnet(
        """
          {
            product_id: std.toString(data.asset_id), 
            title: data.asset_title
          }
        """,
        raw_rec)
    assert output == {"product_id": "123", "title": 'title 123'}
    # merge
    output = eval_jsonnet(
        """
            {
                product_id: std.toString(data.asset_id), 
                title: data.asset_title
    
            } + {custom_attributes: data}
        """, raw_rec
    )

    assert output == {"product_id": "123",
                      "title": 'title 123',
                      "custom_attributes": raw_rec}
