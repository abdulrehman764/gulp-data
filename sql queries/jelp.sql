SELECT
	createdat,
	split_part(split(createdat, ' ')[1], ' ', 1) AS date,
	date_format(date_parse(split_part(split(createdat, ' ')[1], ' ', 1), '%Y-%m-%d'), '%Y') AS Year,
	date_format(date_parse(split_part(split(createdat, ' ')[1], ' ', 1), '%Y-%m-%d'), '%m') AS Month,
	date_format(date_parse(split_part(split(createdat, ' ')[1], ' ', 1), '%Y-%m-%d'), '%d') AS Day,
	ceil(cast(date_format(date_parse(split_part(split(createdat, ' ')[1], ' ', 1), '%Y-%m-%d'), '%d') as integer) / 7.0) AS WeekOfMonth,
	COUNT(deviceid) OVER (PARTITION BY date_format(date_parse(split_part(split(createdat, ' ')[1], ' ', 1), '%Y-%m-%d'), '%Y'), date_format(date_parse(split_part(split(createdat, ' ')[1], ' ', 1), '%Y-%m-%d'), '%m'), ceil(cast(date_format(date_parse(split_part(split(createdat, ' ')[1], ' ', 1), '%Y-%m-%d'), '%d') as integer) / 7.0)) as Device_Count_Per_Week,
	element_at(coordinates, 1) as coordinates,
	element_at(element_at(coordinates, 1), 1) as latitude,
    element_at(element_at(coordinates, 1), 2) as longitude,
	model as Device_Model,
	brand as Device_Brand,
	deviceid as Device_ID,
	_id."$oid" as OS_ID,
	osversion as OS_Version,
	updatedat,
	connectiontype,
	wifi,
	storage_mb_remainig,
	cpu,
	imei,
	phone,
	language,
	carrier,
	ram_mb_remainig,
	bluetooth
FROM "jelps-trip-db"."trips_json_gulp_data_3227"
WHERE
    connectiontype IS NOT NULL AND
    wifi IS NOT NULL AND
    storage_mb_remainig IS NOT NULL AND
    cpu IS NOT NULL AND
    imei IS NOT NULL AND
    phone IS NOT NULL AND
    language IS NOT NULL AND
    carrier IS NOT NULL AND
    ram_mb_remainig IS NOT NULL AND
    bluetooth IS NOT NULL
order by createdat




















--- 1 july

SELECT 
    _id,
    substr(createdat, 1, 4) AS year,
    substr(createdat, 6, 2) AS month,
    substr(createdat, 9, 2) AS day,
    sum(cardinality(coordinates)) AS Unique_Ping_Count
    -- ,cardinality(flatten(coordinates)) AS coord
    
FROM 
    "jelps-trip-db"."trips_json_gulp_data_3227"
group by _id, substr(createdat, 1, 4),substr(createdat, 6, 2),substr(createdat, 9, 2)
order by substr(createdat, 1, 4) desc, substr(createdat, 6, 2) desc, substr(createdat, 9, 2) desc
LIMIT 100;



