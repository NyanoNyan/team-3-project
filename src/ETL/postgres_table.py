def create_tables(conn, cur):
    print('creating tables')
    
    try: 
        cur.execute("""
        CREATE TABLE IF NOT EXISTS size(
            size_id INT IDENTITY(1, 1) NOT NULL,
            size_name VARCHAR(255) NOT NULL,
            PRIMARY KEY(size_id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_name(
            product_name_id INT IDENTITY(1, 1) NOT NULL,
            product_name VARCHAR NOT NULL,
            PRIMARY KEY(product_name_id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_detail (
            product_detail_id INT IDENTITY(1, 1) NOT NULL,
            size_id INT references size(size_id) NOT NULL,
            product_name_id INT references product_name(product_name_id) NOT NULL,
            price FLOAT NOT NULL,
            PRIMARY KEY(product_detail_id)
        );
        """)
        # 
        cur.execute("""
        CREATE TABLE IF NOT EXISTS payment_type(
            payment_type_id INT IDENTITY(1, 1) NOT NULL,
            method VARCHAR(255) NOT NULL,
            PRIMARY KEY(payment_type_id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS branch(
            branch_id INT IDENTITY(1, 1) NOT NULL,
            location VARCHAR(255) NOT NULL,
            PRIMARY KEY(branch_id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS transaction(
            transaction_id INT IDENTITY(1, 1) NOT NULL,
            payment_type_id INT references payment_type(payment_type_id) NOT NULL,
            branch_id INT references branch(branch_id) NOT NULL,
            time_stamp  TIMESTAMP ,
            total_price FLOAT NOT NULL,
            PRIMARY KEY(transaction_id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS basket(
            transaction_id INT references transaction(transaction_id) NOT NULL,
            product_detail_id INT references product_detail(product_detail_id) NOT NULL,
            quantity INT NOT NULL
        );
        """)
    
    except Exception as e:
        print(e)
    
    finally:
        conn.commit()