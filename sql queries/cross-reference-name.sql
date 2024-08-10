SELECT distinct *  
FROM "fortuna-database"."fortuna_full_data"
where job_company_name != '' AND job_company_name LIKE '%waste%' OR job_title LIKE '%waste%'  OR job_title_role LIKE '%waste%' OR
    job_company_name LIKE '%recycle%' OR job_title LIKE '%recycle%'  OR job_title_role LIKE '%recycle%'
    
    
    SELECT CONCAT(first_name, ' ', last_name) AS name FROM "fortuna-database"."fortuna_full_data" limit 100
    select REPLACE("ca_data_brokers"."data broker name", '"', '') AS name from "cross-reference-database"."ca_data_brokers"
    select distinct replace(legal_full_name,'"', '') from "cross-reference-database"."tx_registererd_data_brokers"
    
    
    
    -- Query to get names from the first table
WITH fortuna_names AS (
    SELECT CONCAT(first_name, ' ', last_name) AS fortuna_name , *
    FROM "fortuna-database"."fortuna_full_data"
),

-- Query to get names from the second table
ca_broker_names AS (
    SELECT REPLACE("ca_data_brokers"."data broker name", '"', '') AS broker_name , *
    FROM "cross-reference-database"."ca_data_brokers"
)

-- Joining the two result sets on the 'name' column
SELECT *
FROM fortuna_names f
JOIN ca_broker_names c
ON f.fortuna_name = c.broker_name;








  select distinct replace(legal_full_name,'"', '') from "cross-reference-database"."tx_registererd_data_brokers"
    
    -- Query to get names from the first table
WITH fortuna_names AS (
    SELECT CONCAT(first_name, ' ', last_name) AS fortuna_name , *
    FROM "fortuna-database"."fortuna_full_data"
),

-- Query to get names from the second table
tx_registererd_data_brokers_names AS (
    SELECT CONCAT("tx_registererd_data_brokers"."contact_middle_name", ' ', "tx_registererd_data_brokers"."contact_last_name")  AS legal_full_name1, *
    FROM "cross-reference-database"."tx_registererd_data_brokers"
)

-- Joining the two result sets on the 'name' column
SELECT distinct *
FROM fortuna_names f
JOIN tx_registererd_data_brokers_names c
ON f.fortuna_name = c.legal_full_name1;