
#Make changes to dim_products
#+--------------------------+-------------------+
#| weight_class VARCHAR(255)| weight range(kg)  |
#+--------------------------+-------------------+
#| Light            |   1   | < 2               |
#| Mid_Sized        |   20  | >= 2 - < 40       |
#| Heavy            |   10  | >= 40 - < 140     |
#| Truck_Required   |   70  | => 140            |
#+----------------------------+-----------------+

# New column weight class added based on approximate weight average for each category
# Integer 255 assigned to VARCHAR.