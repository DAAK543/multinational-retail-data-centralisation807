 # Merging latitude columns inj dim_store_details
#+---------------------+-------------------+------------------------+
#| store_details_table | current data type |   required data type   |
#+---------------------+-------------------+------------------------+
#| longitude           | TEXT              | FLOAT                  |
#| locality            | TEXT              | VARCHAR(255)           |
#| store_code          | TEXT              | VARCHAR(?)             |
#| staff_numbers       | TEXT              | SMALLINT               |
#| opening_date        | TEXT              | DATE                   |
#| store_type          | TEXT              | VARCHAR(255) NULLABLE  |
#| latitude            | TEXT              | FLOAT                  |
#| country_code        | TEXT              | VARCHAR(255)           |
#| continent           | TEXT              | VARCHAR(255)           |
#+---------------------+-------------------+------------------------+

# df['final'] = '[' + df['Latitude'].astype(str) + ', ' + df['Longitude'].astype(str) + ']'
