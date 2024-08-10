SELECT distinct * FROM "mmlist-gps-6554"."2024_03_12_13_intent_daily_summary_qty_114997031_csv"  limit 1000000; --114997031
SELECT distinct count(*) FROM "mmlist-gps-6554"."2024_03_12_13_mobilegraph_qty_116896913_csv" limit 10000; --116896914
SELECT distinct count(*) FROM "mmlist-6554"."411_canada_qty_2270136_csv" limit 10; --2270136
SELECT distinct count(*) FROM "mmlist-6554"."411_usa_qty_23134530_csv" limit 10; --23134531
SELECT * FROM "mmlist-6554"."assessor_january_2024_qty_6279940_csv" where mml_id is not null ; --6279940
SELECT distinct count(*) FROM "mmlist-6554"."canada_business_no_email_qty_176881_csv" limit 10; --176881
SELECT distinct * FROM "mmlist-6554"."connects_business_add_qty_9390607_csv" limit 10; --9390607
SELECT distinct * FROM "mmlist-6554"."connects_business_disconnect_qty_7037784_csv" limit 10; --7037784
SELECT distinct * FROM "mmlist-6554"."connects_residential_add_qty_20826220_csv" limit 10;  --20826220




SELECT distinct count(*) 
FROM "mmlist-6554"."2024_03_12_13_intent_daily_summary_qty_114997031_csv" its join 
"mmlist-6554"."2024_03_12_13_mobilegraph_qty_116896913_csv" mgq  on its.mmlid20 = mgq.mmlid20 

limit 10; --114997031




create view intent_summary_mobile_graph as
select distinct  
    t1.email ,
    t1.mmlid20,
    t1.pagelevelcategory1,
    t1.pagelevelcategory2,
    t1.pagelevelcategory3,
    t1.pagelevelcategory4,
    t1.pagelevelcategory5,
    t1.pagelevelcategory6,
    t1.domain,
    t1.geolat,
    t1.geolong,
    t1.count,
    -- t2.mmlid20 AS t2_mmlid20,
    -- t2.email AS t2_email,
    t2.ip as mobilegraph_qty_ip,
    t2.deviceid,
    t2.devicetype,
    t2.date as mobilegraph_qty_date
    -- -- t3.email AS t3_email,
    -- t3.first_name,
    -- t3.last_name,
    -- t3.address,
    -- t3.city,
    -- t3.province,
    -- t3.postal_code,
    -- t3.phone_number,
    -- t3.ip_address as canada_qty_ip,
    -- t3.date AS canada_qty_date
FROM 
    "mmlist-6554"."2024_03_12_13_intent_daily_summary_qty_114997031_csv" t1
JOIN 
    "mmlist-6554"."2024_03_12_13_mobilegraph_qty_116896913_csv" t2 on t1.email = t2.email limit 100000
JOIN 
    "mmlist-6554"."411_canada_qty_2270136_csv" t3 ON t1.email = t3.email
    
    
select * from intent_summary_mobile_graph where geolat is not null  order by mobilegraph_qty_date desc  limit 100000

select distinct *

FROM 
    "mmlist-6554"."connects_residential_add_qty_20826220_csv" t1 
JOIN 
    "mmlist-6554"."connects_business_disconnect_qty_7037784_csv" t2 on t1.listing_name = t2.listing_name
JOIN 
    "mmlist-6554"."connects_business_add_qty_9390607_csv" t3 ON t1.listing_name = t3.listing_name
limit 100



select distinct count(*) from "mmlist-6554"."411_canada_qty_2270136_csv"  limit 100000
select distinct count(*) from "mmlist-6554"."411_usa_qty_23134530_csv"  limit 100000
select distinct count(*) from "mmlist-6554"."411_canada_qty_2270136_csv"  limit 100000
select distinct count(*) from "mmlist-6554"."assessor_january_2024_qty_6279940_csv"  limit 100000


select distinct count(*) from "mmlist-6554"."canada_business_no_email_qty_176881_csv"  limit 100000
select distinct count(*) from "mmlist-6554"."connects_business_add_qty_9390607_csv"  limit 100000
select distinct count(*) from "mmlist-6554"."connects_business_disconnect_qty_7037784_csv"  limit 100000
select distinct count(*) from "mmlist-6554"."connects_residential_add_qty_20826220_csv"  limit 100000


select * from "mmlist-6554"."2024_03_12_13_mobilegraph_qty_116896913_csv" order by date desc limit 100000

select distinct count(*) from intent_summary_mobile_graph 


create view usa_canada_joined_on_name_unfiltered as
SELECT distinct
    
    c.first_name,
    c.last_name,
    c.email,
    c.address,
    c.city,
    c.province AS region,
    c.postal_code,
    c.phone_number AS phone_number_canada,
    c.ip_address,
    c.date,
    u.phone_number AS phone_number_usa
FROM 
    "mmlist-6554"."411_canada_qty_2270136_csv" c
JOIN 
    "mmlist-6554"."411_usa_qty_23134530_csv" u
ON 
    c.first_name = u.first_name
    AND c.last_name = u.last_name
where  c.email !='""'



select * from usa_canada_joined_on_name_unfiltered order by date desc limit 100




SELECT distinct
    
    c.first_name,
    c.last_name,
    c.email,
    c.address,
    c.city,
    c.province AS region,
    c.postal_code,
    c.phone_number AS phone_number_canada,
    c.ip_address,
    c.date,
    u.phone_number AS phone_number_usa
FROM 
    "mmlist-gps-6554"."411_canada_qty_2270136_csv" c
JOIN 
    "mmlist-gps-6554"."411_usa_qty_23134530_csv" u
ON 
    c.first_name = u.first_name
    AND c.last_name = u.last_name
where  c.email !='""'
order by c.date desc 