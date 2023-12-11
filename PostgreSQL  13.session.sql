SELECT locality, COUNT(*) AS store_count
FROM legacy_store_details
GROUP BY locality
ORDER BY store_count DESC;





    
