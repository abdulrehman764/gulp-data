SELECT DISTINCT 
    REPLACE(email, '"', '') AS email, 
    REPLACE(location, '"', '') AS location
FROM "blitz-6724"."6724"
WHERE REPLACE(location, '"', '') != '' and REPLACE(location, '"', '') is not null and REPLACE(location, '"', '') != '"' and  REPLACE(location, '"', '') = 'UNITED STATES' or REPLACE(location, '"', '') = 'US'
GROUP BY 
    REPLACE(email, '"', ''), 
    REPLACE(location, '"', '');
    



SELECT 
    REPLACE(location, '"', '') AS country,
    COUNT(DISTINCT REPLACE(email, '"', '')) AS email_count
FROM "blitz-6724"."6724"
GROUP BY REPLACE(location, '"', '');
