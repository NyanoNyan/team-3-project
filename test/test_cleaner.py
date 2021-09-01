from src.ETL.cleaner import Cleaner

cleaner = Cleaner()

    
def test_replace_blanks():
    item_value = ",coffee,2.95"
    expected_value = "regular,coffee,2.95"
    
    result = cleaner.replace_blanks(item_value)
    
    assert result == expected_value
    
def test_change_case():
    item_value = "Regular,Coffee,2.95"
    expected_value = "regular,coffee,2.95"
    
    result = cleaner.change_case(item_value)
    
    assert result == expected_value
    
def test_remove_card_number():
    item_value = "visa,35538383583583"
    expected_value = "visa"
    
    result = cleaner.remove_card_number(item_value)
    
    assert result == expected_value
    
def test_remove_numers_card_type():
    item_value = "visa12"
    expected_value = "visa"
    
    result = cleaner.remove_numbers_card_type(item_value)
    
    assert result == expected_value
