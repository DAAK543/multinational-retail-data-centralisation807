
import pandas as pd

# Update dim_products table to the required data types

#+-----------------+--------------------+--------------------+
#|  dim_products   | current data type  | required data type |
#+-----------------+--------------------+--------------------+
#| product_price   | TEXT               | FLOAT              |
#| weight          | TEXT               | FLOAT              |
#| EAN             | TEXT               | VARCHAR(255)       |
#| product_code    | TEXT               | VARCHAR(255)       |
#| date_added      | TEXT               | DATE               |
#| uuid            | TEXT               | UUID               |
#| still_available | TEXT               | BOOL               |
#| weight_class    | TEXT               | VARCHAR(255)       |
#+-----------------+--------------------+--------------------+


#print(df)


def dim_products ():
#create table orders_table

 df = pd.DataFrame

 df.dtypes 

("""
    product_price     TEXT,
    Weight            TEXT,
    EAN               TEXT,
    product_code      TEXT,
    date_added        TEXT,
    uuid              TEXT,
    still_available   TEXT,
    weight_class      TEXT

""")

replacements = {
    'TEXT': 'FLOAT',
    'TEXT': 'VARCHAR(255)',
    'TEXT': 'DATE',
    'TEXT': 'UUID',
    'TEXT': 'BOOL'
}


print(replacements)


