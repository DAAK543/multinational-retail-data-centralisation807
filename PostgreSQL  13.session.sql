SELECT
   COUNT(*) AS online_sales_count 
FROM
   orders_table 
WHERE
   store_code IS NOT NULL 
GROUP BY
   product_quantity;




    
