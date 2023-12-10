import pandas as pd

# Cast the columns of the orders_table to the right data types
#+------------------+--------------------+--------------------+
#|   orders_table   | current data type  | required data type |
#+------------------+--------------------+--------------------+
#| date_uuid        | TEXT               | UUID               |
#| user_uuid        | TEXT               | UUID               |
#| card_number      | TEXT               | VARCHAR(255)       |
#| store_code       | TEXT               | VARCHAR(255)       |
#| product_code     | TEXT               | VARCHAR(255)       |
#| product_quantity | BIGINT             | SMALLINT           |
#+------------------+--------------------+--------------------+


#print(df)


def orders_table ():
#create table orders_table

 df = pd.DataFrame

 df.dtypes 

("""
    date_uuid     TEXT
    user_uuid     TEXT
    card_number   TEXT
    store_code:   TEXT
    product_code  TEXT
    product_code  BIGINT
""")

replacements = {
    'TEXT': 'UUID',
    'TEXT': 'VARCHAR(255)',
    'BIGINT': 'SMALLINT'
}

print(replacements)

#col_str = ",".join("{}{}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))

#col_str