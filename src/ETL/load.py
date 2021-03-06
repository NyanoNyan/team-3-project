from datetime import datetime

from src.ETL.extract import Extract
from src.ETL.transform import get_unique_item_key, transform_transaction_format, get_unique_item, get_unique_size
# from src.ETL.helper_modules.helper_funcs import pretty_print_dict

def compare_get_query_id(sql, value, cur):
    cur.execute(sql)
    
    row = cur.fetchall()
    # print(row)
    for item in row:
        # print('loop over item:',item)
        if item[1] == value:
            return item[0]

def check_unique(conn, cur, sql_check, sql_check_values, sql_execute, values):
    try:
        # print('In here')
        cur.execute(sql_check, sql_check_values)
        # print('In here 2')
        if cur.fetchone() is None:
            print('There is nothing similar')
            cur.execute(sql_execute, values)
        else:
            print('There is already something similar')
    except Exception as e:
        print('Cannot add: ', e)
    finally:
        conn.commit()


## Skeleton code when loading
def load_test():
    basket = transform_transaction_format()
    sql = """
        INSERT INTO load_test (product_name, quantity)
        VALUES (%s, %s)
    """

    for data in basket:
        print(data)

##############################
# Product side
def load_size(data, conn, cur):
    sql_execute = \
        '''
            INSERT INTO size (size_name)  
            VALUES (%s)
        '''
    
    sql_check = \
        '''
            SELECT size_name FROM size WHERE size_name = %s
        '''

    # sizes = ['large', 'regular']
    sizes = get_unique_size(data)

    for one_data in sizes:

        try:
            check_unique(conn, cur, sql_check, (one_data,), sql_execute, (one_data,))
        except:
            print('cannot be added')
        finally:
            conn.commit()


def load_product_detail(item, conn, cur):
    sql_product_details = \
    """
        INSERT INTO product_detail(size_id, product_name_id, price)
        VALUES (%s, %s, %s)
    """
    try:
        ## Load product_detail table
        id_for_size = compare_get_query_id('SELECT * FROM size', item['size'], cur)
        id_for_product_name = compare_get_query_id('SELECT * FROM product_name', item['name'], cur)
        values = (id_for_size, id_for_product_name, float(item['price']))
        
        # print('test:', id_for_size, id_for_product_name)
        cur.execute(F"""
                        SELECT * FROM product_detail
                        WHERE size_id = {id_for_size} AND product_name_id = {id_for_product_name} AND price = {item['price']}
                    """)
        
        if cur.fetchone() is None:
            print('There is nothing similar')
            cur.execute(sql_product_details, values, )
        else:
            print('There is already something similar')

    except Exception as e:
        print('Cannot add to product_detail', e)
    finally:
        conn.commit()


def load_product_side(data, conn, cur):
    # Get unique products with size and size
    unique_item = get_unique_item(data)
    
    # pretty_print_dict(unique_item)

    sql_check = \
        '''
            SELECT * FROM product_name WHERE product_name = %s
        '''
    
    sql_product_name = \
        """
            INSERT INTO product_name(product_name)
            VALUES (%s)
        """

    for item in unique_item:
        try:
            ## Load product_name table
            check_unique(conn, cur, sql_check, (item['name'], ), sql_product_name, (item['name'], ))
            # cur.execute(sql_product_name, (item['name'], ))
        except Exception as e:
            print('Cannot add this product: ', e)

        finally:
            conn.commit()
        
        load_product_detail(item, conn, cur)


###################################
# Transaction side
def load_branch(data, conn, cur):
    unique_branch = get_unique_item_key('store_location', data)
    # print(unique_branch)
    
    sql_check = \
        '''
            SELECT location FROM branch WHERE location = %s
        '''
    
    sql = \
        """
            INSERT INTO branch(location)
            VALUES(%s)
        """

    for branch in unique_branch: 
        try:
            check_unique(conn, cur, sql_check, (branch, ), sql, (branch, ))
            # cur.execute(sql, (branch,))

        except Exception as e:
            print('Cannot add branch', e)
        finally:
            conn.commit()


def load_payment_type(data, conn, cur):
    unique_payment = get_unique_item_key('payment_type', data)
    
    sql_check = \
    '''
        SELECT method FROM payment_type WHERE method = %s
    '''
    
    sql = \
        """
            INSERT INTO payment_type(method)
            VALUES(%s)
        """
    
    for payment in unique_payment:
        try:
            check_unique(conn, cur, sql_check, (payment, ), sql, (payment, ))
            # cur.execute(sql, (payment,) )

        except Exception as e:
            print('Cannot add payment', e)
        finally:
            conn.commit()


def basket_load(transaction_with_quantity, transaction_id, count, conn, cur):
        sql_basket = \
        """
            INSERT INTO basket (transaction_id, product_detail_id, quantity)
            VALUES (%s, %s, %s)
        """

        for basket_list in transaction_with_quantity[count]:
            for key, value in basket_list.items():
                key_split = key.split(',')
                # print(key_split, value)
                size_id = compare_get_query_id('SELECT * FROM size', key_split[0], cur)
                name_id = compare_get_query_id('SELECT * FROM product_name', key_split[1], cur)
                
                try:
                    # print(size_id, name_id)
                    cur.execute(f"""
                                    SELECT product_detail_id
                                    FROM product_detail
                                    WHERE size_id = {size_id} AND product_name_id = {name_id}
                                """)
                    product_detail_id = cur.fetchone()[0]
                    # print('details', product_detail_id)
                    sql_value = (transaction_id, product_detail_id, int(value),)
                    # print(sql_value)
                    # print(sql_basket)
                    cur.execute(sql_basket, sql_value)

                    conn.commit()
                except Exception as e:                    
                    print('Cannot add basket_load: ', e)
                    print('The cur includes: ', cur.fetchone())


def load_transaction_side(data, conn, cur):
    extract = Extract()
    # data = extract.extract_dict("../../data/cleaned_data.csv")
    transaction_with_quantity = transform_transaction_format(data)
    
    sql = \
        """
            INSERT INTO transaction (payment_type_id, branch_id, time_stamp, total_price)
            VALUES (%s, %s, %s, %s)
        """
    load_branch(data, conn, cur)
    load_payment_type(data, conn, cur)
    # load_card_type(data)
    
    count = 0
    for each_transaction in data:
        # print(each_transaction)
        payment_type_val = compare_get_query_id('SELECT * FROM payment_type', each_transaction['payment_type'], cur)
        branch_val = compare_get_query_id('SELECT * FROM branch', each_transaction['store_location'], cur)
        # card_type_val = compare_get_query_id('SELECT * FROM card_type', each_transaction['card_type'], cur)
        # print(each_transaction['timestamp'])
        time = datetime.strptime(each_transaction['timestamp'], '%d/%m/%Y %H:%M')
        
        values = (payment_type_val, branch_val, time, float(each_transaction['total_price']))
        # print('values: ',values)
        try:
            cur.execute(sql, values, )
            cur.execute('SELECT MAX(transaction_id) FROM transaction')
            transaction_id = cur.fetchone()[0]
        except Exception as e:
            print('Cannot add transaction', e)
        finally:
            conn.commit()
        
        basket_load(transaction_with_quantity, transaction_id, count, conn, cur)
        
        count += 1 


def load_data(data, conn, cur): 
    # Load data
    load_size(data, conn, cur)
    load_product_side(data, conn, cur)
    print('Transaction side loading')
    load_transaction_side(data, conn, cur)