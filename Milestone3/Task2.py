
import pandas as pd

# Cast the columns of the dim_user_table to the right data types
#+----------------+--------------------+--------------------+
#| dim_user_table | current data type  | required data type |
#+----------------+--------------------+--------------------+
#| first_name     | TEXT               | VARCHAR(255)       |
#| last_name      | TEXT               | VARCHAR(255)       |
#| date_of_birth  | TEXT               | DATE               |
#| country_code   | TEXT               | VARCHAR(255)       |
#| user_uuid      | TEXT               | UUID               |
#| join_date      | TEXT               | DATE               |
#+----------------+--------------------+--------------------+

#print(df)


def dim_user_table ():
 
#create table dim_users_table

 df = pd.DataFrame

 df.dtypes
 
# current data type for dim_users_table
("""
    first_name     TEXT
    last_name      TEXT
    date_of_birth  TEXT
    country_code   TEXT
    user_uuid      TEXT
    join_date      TEXT
""")

replacements = {
    'TEXT': 'DATE',
    'TEXT': 'VARCHAR(255)',
    'TEXT': 'UUID'
}


print(replacements)


#col_str = ",".join("{}{}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))

#col_str