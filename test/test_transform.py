from src.ETL.transform import transform_transaction_format, transform_unique_product, get_unique_item, get_unique_item_key, chunks
from unittest import mock

test_data = [{'timestamp': '2021-02-23 09:00:48', 
             'store_location': 'Isle of Wight', 
             'payment_type': 'CASH',
             'total_price': 8.4, 
             'card_type': 'None',
             'basket': 'large,hot chocolate,2.9,large,chai latte,2.6,large,hot chocolate,2.9'},
             {'timestamp': '2021-02-23 09:01:45', 'store_location': 'Isle of Wight', 'payment_type': 'CARD', 'total_price': 2.45, 'card_type': 'discover', 'basket': 'large,latte,2.45'}]

def test_transform_transaction_format():
    expected_value = [[{'large,hot chocolate': 2, 'large,chai latte': 1}],
                    [{'large,latte':1}]]
    
    result = transform_transaction_format(test_data)
    
    assert expected_value == result
    
def test_get_unique_item_key():
    expected_value = {"CARD", "CASH"}
    
    result = get_unique_item_key("payment_type", test_data) 
    
    assert result == expected_value   
    
def test_transform_unique_product():
    mock_chunks = mock.MagicMock()
    mock_chunks.return_value = [["large", "hot chocolate", "2.9"],["large", "latte", "2.45"]]
    
    
    expected_value = [{"size": "large", "name":'hot chocolate', "price":'2.9'},
                    {'name':'latte', 'price':'2.45', 'size': 'large'}]
    
    result = transform_unique_product(test_data, mock_chunks)
    
    assert result == expected_value
    


def test_get_unique_item():
    
    mock_transform_unique_product = mock.MagicMock()
    mock.return_value = [{"size": "large", "name":'hot chocolate', "price":'2.9'},
                    {'name':'latte', 'price':'2.45', 'size': 'large'},
                    {"size": "large", "name":'hot chocolate', "price":'2.9'}]
    
    expected_value = [{"size": "large", "name":'hot chocolate', "price":'2.9'},
                    {'name':'latte', 'price':'2.45', 'size': 'large'}]
    
    result = get_unique_item(test_data, mock_transform_unique_product)
    
    assert result == expected_value