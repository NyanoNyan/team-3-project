from ETL.helper_modules.helper_funcs import pretty_print_dict
from ETL.cleaner import Cleaner
from ETL.extract import Extract

# This needs to run just once, need to update
def clean_data(data):
    extract = Extract()
    cleaner  = Cleaner()
    transaction_df = extract.extract_pandas(data)
    
    transaction_df.drop(columns="customer_name", inplace=True)
    
    # Remove card number, change case, replace blanks
    transaction_df["card_type"] = transaction_df["card_type"].apply(lambda x: cleaner.remove_card_number(x))
    transaction_df["basket"] = transaction_df["basket"].apply(lambda x: cleaner.replace_blanks(x))  
    transaction_df["card_type"] = transaction_df["card_type"].apply(lambda x: cleaner.remove_numbers_card_type(x))    
    transaction_df["basket"] = transaction_df["basket"].apply(lambda x: cleaner.change_case(x))   
    
    # Reorder the columns
    transaction_df = transaction_df[["timestamp", "store_location","payment_type", "total_price", "card_type", "basket"]]
#    transaction_df.to_csv('../../data/cleaned_data.csv', sep=',', index=False)
    return transaction_df.to_dict("records")
#######################################################################
# Get the product quantity for each transaction

def transform_transaction_format(data):
    extract = Extract()
    # data = extract.extract_dict("../../data/cleaned_data.csv")
    store = []
    
    # This is used to make the basket table, each list contains each transaction,
    # which then has the specific quanity for that particular item

    for each_transaction in data:
        for keys in each_transaction:
            if keys == "basket":
                item_name = []
                store_unique_item_quanity = {}
                item_list = each_transaction[keys].split(',')

                for i in list(range(0, len(item_list), 3)):
                    item_name.append(item_list[i] + ',' + item_list[i+1])
                unique_item = set(item_name)
                
                for item in unique_item:
                    store_unique_item_quanity[item] = item_name.count(item)
                
                store.append([store_unique_item_quanity])
    return store

def get_unique_item_key(column_name, data):
    extract = Extract()
    # data = extract.extract_dict("../../data/cleaned_data.csv")
    # print(data)
    store = []
    
    for each_transaction in data:
        for keys in each_transaction:
            if keys == column_name:
                if each_transaction[keys] not in store:
                    store.append(each_transaction[keys])
                else:
                    continue
    return set(store)

# print(get_unique_payment_type('store_location'))

#############################################################################
# Get the unique product, size and price
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def transform_unique_product(data, chunk_func):
    extract = Extract()
    # data = extract.extract_dict("../../data/cleaned_data.csv")
    item_store_list = []
    
    for each_transaction in data:
        for keys in each_transaction:
            if keys == "basket":
                item_store = {}
                item_list = each_transaction[keys].split(',')
                
                for item in chunks(item_list, 3):
                    item_store['size'] = item[0]
                    item_store['name'] = item[1]
                    item_store['price'] = item[2]

                item_store_list.append(item_store)
    return item_store_list

def get_unique_item(data, transform_func):
    store = []
    transactions = transform_unique_product(data, chunks)
    for trans in transactions:
        in_store = False
        for item in store:
            if trans["size"] == item["size"] and trans["name"] == item["name"]:
                in_store = True
                break
            else:
                continue
        if in_store == False:
            store.append(trans)
        else:
            continue
    return store

#########################################################################################

def basket_to_dict(basket):
    basket_split = basket.split(",")
    store = []
    
    for basket in chunks(basket_split, 3):
        in_store = False
        
        for item in store:
            
            if basket[0] == item["size"] and basket[1] == item["name"] and basket[2] == item["price"]:
                in_store = True
                item["quantity"] += 1
                break
       
            else:
                continue
       
        if in_store == False:
            item_dict = {"size" : basket[0], "name" : basket[1],
                        "price" : basket[2], "quantity": 1}
            store.append(item_dict)
        else:
            continue
    print(store)
    return store
    
def apply_basket_to_dict(transaction_df):
    
    transaction_df["basket"] = transaction_df["basket"].apply(lambda x: basket_to_dict(x))       
    
    print(transaction_df["basket"])
    
    return transaction_df  