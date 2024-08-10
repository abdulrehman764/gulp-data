----14 june

SELECT  distinct
    c.co_id,
    c.co_first_name,
    c.co_last_name,
    c.co_address1,
    c.co_city,
    c.postal_code,
    c.co_phone_home,
    c.co_phone_business, 
    c.co_e_mail,
    c.co_phone_mobile
    

FROM 
    dboconsumers_json c
LEFT JOIN 
    dboconsumer_vehicles_json v ON c.co_id = v.co_id
LEFT JOIN 
    dboauto_sys_notes_json n ON c.co_id = n.co_id
LEFT JOIN 
    dboconsumer_favorite_destinations_json f ON c.co_id = f.co_id
LEFT JOIN 
    dboconsumer_favorite_spaces_json s ON c.co_id = s.co_id
LEFT JOIN 
    dboconsumer_gift_cards_json g ON c.co_id = g.co_id
LEFT JOIN 
    dbocustomer_consumer_credit_cards_json cr ON c.co_id = cr.co_id
LEFT JOIN 
    dbocustomer_consumers_json cu ON c.co_id = cu.co_id
LEFT JOIN 
    dboorder_groups_json og ON c.co_id = og.co_id
LEFT JOIN 
    dboorder_item_fulfillments_json ofl ON c.co_id = ofl.co_id
LEFT JOIN 
    dboorder_logs_json olj ON c.co_id = CAST(json_extract_scalar(olj.orl_xml, '$.consumer_id') AS INT)
where c.co_e_mail != ''











----11 july

SELECT distinct *
FROM "leisure-4037-refreshed-data-without-timestamp-crawler-files"."dbocustomers_json" AS customers
JOIN "leisure-4037-refreshed-data-without-timestamp-crawler-files"."dboconsumers_json" AS consumers
ON customers.cu_consumer_email_address = consumers.co_e_mail 




SELECT *
FROM (
    SELECT DISTINCT *
    FROM "leisure-4037-refreshed-data-without-timestamp-crawler-files"."dbocustomers_json" AS customers
    JOIN "leisure-4037-refreshed-data-without-timestamp-crawler-files"."dboconsumers_json" AS consumers
    ON customers.cu_consumer_email_address = consumers.co_e_mail
    
    UNION
    
    SELECT DISTINCT *
    FROM "leisure-4037-refreshed-data-without-timestamp-crawler-files"."dbocustomers_json" AS customers
    JOIN "leisure-4037-refreshed-data-without-timestamp-crawler-files"."dboconsumers_json" AS consumers
    ON customers.cu_support_email_address = consumers.co_e_mail
) AS union_table;