select distinct u.email,
	u.full_name,
	u.lastname,
	u.username,
	u.phone,
	u.gender,
	u.dateofbirth,
	u.deviceinformation,
	cdld.uniqueid as deviceid,
	cdld.idtype as device_type,
	--cdld.utc_timestamp as created_time,
	--FROM_UNIXTIME(cdld.utc_timestamp / 1000) AS created_time,
	DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%Y-%m-%d') AS created_time,
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%Y') AS Year,
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%m') AS Month,
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d') AS Day,
    ceil(cast(date_format(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d') as integer) / 7.0) AS WeekOfMonth,
    COUNT(cdld.uniqueid) OVER (PARTITION BY DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%Y'), DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%m'), ceil(cast(date_format(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d') as integer) / 7.0)) as Device_Count_Per_Week,
    --CEIL(DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d') / 7) AS WeekOfMonth,
	cdld.ipaddress as device_ipaddress,
	cdld.countrycode as device_country_code,
	u.firstordermadeat,
	u.lastordermadeat,
	u.totalorderplaced,
	ul.name as address_type,
	CONCAT_WS(', ', ul.address, ul.number, ul.town, ul.zip) AS full_address,
	cj.status as order_status,
	cj.total as amount_spent --ul.address as address, ul.number as street, ul.town as town, ul.zip as zip
FROM user_json u
	JOIN userlocation_json ul ON u._id = REPLACE(ul._p_user, '_User$', '')
	JOIN cart_json cj ON u._id = REPLACE(cj._p_user, '_User$', '')
	JOIN collectdevicelocationdata_json cdld ON u._id = REPLACE(cdld._p_user, '_User$', '')
where u.dateofbirth != ''
	and u.gender != ''
limit 5000





remove addresstype, full_address, order_status and sum(amount_spent) in select


SELECT DISTINCT
    u.email,
    u.full_name,
    u.lastname,
    u.username,
    u.phone,
    u.gender,
    u.dateofbirth,
    u.deviceinformation,
    cdld.uniqueid AS deviceid,
    cdld.idtype AS device_type,
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%Y-%m-%d') AS created_time,
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%Y') AS Year,
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%m') AS Month,
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d') AS Day,
    CEIL(CAST(DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d') AS INTEGER) / 7.0) AS WeekOfMonth,
    COUNT(cdld.uniqueid) OVER (PARTITION BY
        DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%Y'),
        DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%m'),
        CEIL(CAST(DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d') AS INTEGER) / 7.0)
    ) AS Device_Count_Per_Week,
    cdld.ipaddress AS device_ipaddress,
    cdld.countrycode AS device_country_code,
    u.firstordermadeat,
    u.lastordermadeat,
    u.totalorderplaced,
    ul.name AS address_type,
    CONCAT_WS(', ', ul.address, ul.number, ul.town, ul.zip) AS full_address,
    cj.status AS order_status,
    SUM(cj.total) AS amount_spent
FROM user_json u
JOIN userlocation_json ul ON u._id = REPLACE(ul._p_user, '_User$', '')
JOIN cart_json cj ON u._id = REPLACE(cj._p_user, '_User$', '')
JOIN collectdevicelocationdata_json cdld ON u._id = REPLACE(cdld._p_user, '_User$', '')
WHERE u.dateofbirth != ''
    AND u.gender != ''
GROUP BY
    u.email,
    u.full_name,
    u.lastname,
    u.username,
    u.phone,
    u.gender,
    u.dateofbirth,
    u.deviceinformation,
    cdld.uniqueid,
    cdld.idtype,
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%Y-%m-%d'),
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%Y'),
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%m'),
    DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d'),
    CEIL(CAST(DATE_FORMAT(FROM_UNIXTIME(cdld.utc_timestamp / 1000), '%d') AS INTEGER) / 7.0),
    cdld.ipaddress,
    cdld.countrycode,
    u.firstordermadeat,
    u.lastordermadeat,
    u.totalorderplaced,
    ul.name,
    CONCAT_WS(', ', ul.address, ul.number, ul.town, ul.zip),
    cj.status
ORDER BY created_time
LIMIT 5000;





SELECT
  (SELECT COUNT(DISTINCT u._id)
   FROM user_json u) AS unique_users_in_user_json,
  
  (SELECT COUNT(DISTINCT REPLACE(ul._p_user, '_User$', ''))
   FROM userlocation_json ul
   JOIN user_json u ON u._id = REPLACE(ul._p_user, '_User$', '')) AS unique_users_in_userlocation_json,
  
  (SELECT COUNT(DISTINCT REPLACE(cj._p_user, '_User$', ''))
   FROM cart_json cj
   JOIN user_json u ON u._id = REPLACE(cj._p_user, '_User$', '')) AS unique_users_in_cart_json,
  
  (SELECT COUNT(DISTINCT REPLACE(cdld._p_user, '_User$', ''))
   FROM collectdevicelocationdata_json cdld
   JOIN user_json u ON u._id = REPLACE(cdld._p_user, '_User$', '')) AS unique_users_in_collectdevicelocationdata_json;




SELECT DISTINCT
    u.email,
    u.full_name,
    u.lastname,
    u.username,
    u.phone,
    u.gender,
    u.dateofbirth,
    u.deviceinformation,
    u.firstordermadeat,
    u.lastordermadeat,
    u.totalorderplaced
FROM user_json u
JOIN userlocation_json ul ON u._id = REPLACE(ul._p_user, '_User$', '')
GROUP BY
    u.email,
    u.full_name,
    u.lastname,
    u.username,
    u.phone,
    u.gender,
    u.dateofbirth,
    u.deviceinformation,
    u.firstordermadeat,
    u.lastordermadeat,
    u.totalorderplaced
ORDER BY u.firstordermadeat;









-- 25 june 

select DISTINCT email,roles, driveronline, isdriver, isdispatcher, appliedfordriver from user_json where email != '' and isdriver = false or isdriver IS NULL and driveronline != true or driveronline IS NULL






WITH cleaned_data AS (
    SELECT DISTINCT
        substr(_created_at."$date", 1, 10) AS date,
        substr(_created_at."$date", 1, 4) AS year,
        substr(_created_at."$date", 6, 2) AS month,
        substr(_created_at."$date", 9, 2) AS day,
        email
    FROM user_json
    WHERE email != '' 
      AND (isdriver = false OR isdriver IS NULL) 
      AND (driveronline != true OR driveronline IS NULL)
),
yearly_data AS (
    SELECT
        year,
        COUNT(DISTINCT email) AS total_emails_year
    FROM cleaned_data
    GROUP BY year
),
monthly_data AS (
    SELECT
        year,
        month,
        COUNT(DISTINCT email) AS total_emails_month
    FROM cleaned_data
    GROUP BY year, month
),
average_emails AS (
    SELECT
        year,
        AVG(total_emails_month) AS avg_emails_per_month
    FROM monthly_data
    GROUP BY year
)
SELECT 
    md.year,
    md.month,
    yd.total_emails_year,
    ae.avg_emails_per_month,
    yd.total_emails_year / 12.0 AS avg_emails_per_month_from_year -- Approximate average per month based on yearly total
FROM 
    monthly_data md
JOIN 
    yearly_data yd ON md.year = yd.year
JOIN 
    average_emails ae ON md.year = ae.year
ORDER BY 
    md.year, md.month;















-- 1st july


SELECT 
    uniqueid AS _id,
    DATE_FORMAT(FROM_UNIXTIME(utc_timestamp / 1000), '%Y') AS year,
    DATE_FORMAT(FROM_UNIXTIME(utc_timestamp / 1000), '%m') AS month,
    DATE_FORMAT(FROM_UNIXTIME(utc_timestamp / 1000), '%d') AS day,
    COUNT(latitude) AS Unique_Ping_Count,
    COUNT(longitude) AS Unique_Ping_Count
FROM 
    collectdevicelocationdata_json
GROUP BY 
    uniqueid,
    DATE_FORMAT(FROM_UNIXTIME(utc_timestamp / 1000), '%Y'),
    DATE_FORMAT(FROM_UNIXTIME(utc_timestamp / 1000), '%m'),
    DATE_FORMAT(FROM_UNIXTIME(utc_timestamp / 1000), '%d')
ORDER BY 
    year DESC, month DESC, day DESC;

