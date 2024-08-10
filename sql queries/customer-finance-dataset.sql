WITH CTE AS (
    SELECT
        TRIM(REPLACE(c.first, '"', '')) AS first,
        TRIM(REPLACE(c.last, '"', '')) AS last,
        TRIM(REPLACE(c.email, '"', '')) AS email,
        -- TRIM(REPLACE(CONCAT_WS(', ', REPLACE(ca.streetname, '"', ''), REPLACE(ca.city, '"', ''), REPLACE(ca.state, '"', '')), '"', '')) AS address,
        l.customerid,
        CONCAT_WS(', ',
            TRIM(REPLACE(ca.streetnumber, '"', '')),
            TRIM(REPLACE(ca.streetname, '"', '')),
            TRIM(REPLACE(ca.city, '"', '')),
            TRIM(REPLACE(ca.state, '"', ''))
        ) AS address,
        TRIM(REPLACE(c.creditscore, '"', '')) AS creditscore,
        TRIM(REPLACE(l.initpayment, '"', '')) AS initpayment,
        TRIM(REPLACE(l.initpayments, '"', '')) AS initpayments,
        TRIM(REPLACE(acs.workflowtype, '"', '')) AS workflowtype,
        TRIM(REPLACE(acs.amount, '"', '')) AS purchase_amount,
        TRIM(REPLACE(acs.createddate, '"', '')) AS purchase_date,
        TRIM(REPLACE(l.state, '"', '')) AS Geographic_Transaction_State,
        ROW_NUMBER() OVER (PARTITION BY TRIM(REPLACE(c.first, '"', '')), TRIM(REPLACE(c.last, '"', '')), TRIM(REPLACE(c.email, '"', '')), l.customerid ORDER BY TRIM(REPLACE(acs.createddate, '"', '')) DESC) AS rn
    FROM
        dbo_customer_csv c
    JOIN
        dbo_lease_csv l ON c.id = l.customerid
    JOIN
        dbo_customeraddress_csv ca ON ca.customerid = l.customerid
    JOIN
        dbo_accountconfigurationstage_csv acs ON acs.customerid = l.customerid
    WHERE
        TRIM(REPLACE(acs.workflowtype, '"', '')) = 'Purchase'
)
SELECT
    first,
    last,
    email,
    address,
    customerid,
    creditscore,
    initpayment,
    initpayments,
    workflowtype,
    purchase_amount,
    purchase_date,
    Geographic_Transaction_State
FROM CTE
WHERE rn = 1
LIMIT 2000;













--014 junbe


WITH CTE AS (
    SELECT
        TRIM(REPLACE(c.first, '"', '')) AS first,
        TRIM(REPLACE(c.last, '"', '')) AS last,
        TRIM(REPLACE(c.email, '"', '')) AS email,
        l.customerid,
        TRIM(REPLACE(ca.streetnumber, '"', '')) as StreetNumber,
        TRIM(REPLACE(ca.streetname, '"', '')) as StreetName,
        TRIM(REPLACE(ca.city, '"', '')) as City,
        TRIM(REPLACE(ca.state, '"', '')) as State,
        CONCAT_WS(', ',
            TRIM(REPLACE(ca.streetnumber, '"', '')),
            TRIM(REPLACE(ca.streetname, '"', '')),
            TRIM(REPLACE(ca.city, '"', '')),
            TRIM(REPLACE(ca.state, '"', ''))
        ) AS address,
        TRIM(REPLACE(c.creditscore, '"', '')) AS creditscore,
        CASE 
            WHEN TRIM(REPLACE(l.initpayment, '"', '')) = '' THEN 0
            ELSE CAST(TRIM(REPLACE(l.initpayment, '"', '')) AS DECIMAL(10, 2))
        END AS initpayment,
        CASE 
            WHEN TRIM(REPLACE(l.initpayments, '"', '')) = '' THEN 0
            ELSE CAST(TRIM(REPLACE(l.initpayments, '"', '')) AS DECIMAL(10, 2))
        END AS initpayments,
        TRIM(REPLACE(l.state, '"', '')) AS Geographic_Transaction_State
    FROM
        dbo_customer_csv c
    JOIN
        dbo_lease_csv l ON c.id = l.customerid
    JOIN
        dbo_customeraddress_csv ca ON ca.customerid = l.customerid
)
SELECT
    first,
    last,
    email,
    StreetNumber,
    StreetName,
    City,
    State,
    address,
    customerid,
    creditscore,
    SUM(initpayment) AS total_initpayment,
    SUM(initpayments) AS total_initpayments,
    Geographic_Transaction_State
FROM CTE
GROUP BY
    first,
    last,
    email,
    StreetNumber,
    StreetName,
    City,
    State,
    address,
    customerid,
    creditscore,
    Geographic_Transaction_State

limit 35000







WITH CTE AS (
    SELECT
        TRIM(REPLACE(c.first, '"', '')) AS first,
        TRIM(REPLACE(c.last, '"', '')) AS last,
        TRIM(REPLACE(c.email, '"', '')) AS email,
        -- TRIM(REPLACE(CONCAT_WS(', ', REPLACE(ca.streetname, '"', ''), REPLACE(ca.city, '"', ''), REPLACE(ca.state, '"', '')), '"', '')) AS address,
        l.customerid,
        CONCAT_WS(', ',
            TRIM(REPLACE(ca.streetnumber, '"', '')),
            TRIM(REPLACE(ca.streetname, '"', '')),
            TRIM(REPLACE(ca.city, '"', '')),
            TRIM(REPLACE(ca.state, '"', ''))
        ) AS address,
        TRIM(REPLACE(c.creditscore, '"', '')) AS creditscore,
        TRIM(REPLACE(l.initpayment, '"', '')) AS initpayment,
        TRIM(REPLACE(l.initpayments, '"', '')) AS initpayments,
        TRIM(REPLACE(acs.workflowtype, '"', '')) AS workflowtype,
        TRIM(REPLACE(acs.amount, '"', '')) AS purchase_amount,
        TRIM(REPLACE(acs.createddate, '"', '')) AS purchase_date,
        TRIM(REPLACE(l.state, '"', '')) AS Geographic_Transaction_State
        ,ROW_NUMBER() OVER (PARTITION BY TRIM(REPLACE(c.first, '"', '')), TRIM(REPLACE(c.last, '"', '')), TRIM(REPLACE(c.email, '"', '')), l.customerid ORDER BY TRIM(REPLACE(acs.createddate, '"', '')) DESC) AS rn
    FROM
        dbo_customer_csv c
    JOIN
        dbo_lease_csv l ON c.id = l.customerid
    JOIN
        dbo_customeraddress_csv ca ON ca.customerid = l.customerid
    JOIN
        dbo_accountconfigurationstage_csv acs ON acs.customerid = l.customerid
    WHERE
        TRIM(REPLACE(acs.workflowtype, '"', '')) = 'Purchase'
)
SELECT
    first,
    last,
    email,
    address,
    customerid,
    creditscore,
    initpayment,
    initpayments,
    workflowtype,
    purchase_amount,
    purchase_date,
    Geographic_Transaction_State
FROM CTE
WHERE rn = 1
LIMIT 2000;
rlpj080896@gmail.com
mamipupe70@gmail.com








-- 19 june 



WITH CTE AS (
    SELECT
        TRIM(REPLACE(c.first, '"', '')) AS first,
        TRIM(REPLACE(c.last, '"', '')) AS last,
        TRIM(REPLACE(c.email, '"', '')) AS email,
        l.customerid,
        TRIM(REPLACE(ca.streetnumber, '"', '')) as StreetNumber,
        TRIM(REPLACE(ca.streetname, '"', '')) as StreetName,
        TRIM(REPLACE(ca.city, '"', '')) as City,
        TRIM(REPLACE(ca.state, '"', '')) as State,
        CONCAT_WS(', ',
            TRIM(REPLACE(ca.streetnumber, '"', '')),
            TRIM(REPLACE(ca.streetname, '"', '')),
            TRIM(REPLACE(ca.city, '"', '')),
            TRIM(REPLACE(ca.state, '"', ''))
        ) AS address,
        TRIM(REPLACE(c.creditscore, '"', '')) AS creditscore,
        CASE 
            WHEN TRIM(REPLACE(l.initpayment, '"', '')) = '' THEN 0
            ELSE CAST(TRIM(REPLACE(l.initpayment, '"', '')) AS DECIMAL(10, 2))
        END AS initpayment,
        CASE 
            WHEN TRIM(REPLACE(l.initpayments, '"', '')) = '' THEN 0
            ELSE CAST(TRIM(REPLACE(l.initpayments, '"', '')) AS DECIMAL(10, 2))
        END AS initpayments,
        CAST(TRIM(REPLACE(acs.amount, '"', '')) AS DECIMAL(10, 2)) AS purchase_amount,
        TRIM(REPLACE(acs.createddate, '"', '')) AS purchase_date,
        TRIM(REPLACE(l.state, '"', '')) AS Geographic_Transaction_State,
        ROW_NUMBER() OVER (PARTITION BY TRIM(REPLACE(c.first, '"', '')), TRIM(REPLACE(c.last, '"', '')), TRIM(REPLACE(c.email, '"', '')), l.customerid ORDER BY TRIM(REPLACE(acs.createddate, '"', '')) DESC) AS rn
    FROM
        dbo_customer_csv c
    JOIN
        dbo_lease_csv l ON c.id = l.customerid
    JOIN
        dbo_customeraddress_csv ca ON ca.customerid = l.customerid
    JOIN
        dbo_accountconfigurationstage_csv acs ON acs.customerid = l.customerid
)
SELECT
    first,
    last,
    email,
    StreetNumber,
    StreetName,
    City,
    State,
    address,
    customerid,
    creditscore,
    SUM(purchase_amount) AS total_purchase_amount,
    purchase_date,
    SUM(initpayment) AS total_initpayment,
    SUM(initpayments) AS total_initpayments,
    Geographic_Transaction_State
FROM CTE
WHERE rn = 1
GROUP BY
    first,
    last,
    email,
    StreetNumber,
    StreetName,
    City,
    State,
    address,
    customerid,
    creditscore,
    purchase_date,
    Geographic_Transaction_State







-- 19 june:



WITH CTE AS (
    SELECT
            
        TRIM(REPLACE(c.first, '"', '')) AS first,
        TRIM(REPLACE(c.last, '"', '')) AS last,
        TRIM(REPLACE(c.email, '"', '')) AS email,
        l.customerid,
        TRIM(REPLACE(ca.streetnumber, '"', '')) as StreetNumber,
        TRIM(REPLACE(ca.streetname, '"', '')) as StreetName,
        TRIM(REPLACE(ca.city, '"', '')) as City,
        TRIM(REPLACE(ca.state, '"', '')) as State,
        TRIM(REPLACE(a.zip, '"', '')) as Zip,
        CONCAT_WS(', ',
            TRIM(REPLACE(ca.streetnumber, '"', '')),
            TRIM(REPLACE(ca.streetname, '"', '')),
            TRIM(REPLACE(ca.city, '"', '')),
            TRIM(REPLACE(ca.state, '"', ''))
        ) AS address,
        TRIM(REPLACE(c.creditscore, '"', '')) AS creditscore,
        CASE 
            WHEN TRIM(REPLACE(l.initpayment, '"', '')) = '' THEN 0
            ELSE CAST(TRIM(REPLACE(l.initpayment, '"', '')) AS DECIMAL(10, 2))
        END AS initpayment,
        CASE 
            WHEN TRIM(REPLACE(l.initpayments, '"', '')) = '' THEN 0
            ELSE CAST(TRIM(REPLACE(l.initpayments, '"', '')) AS DECIMAL(10, 2))
        END AS initpayments,
        CAST(TRIM(REPLACE(acs.amount, '"', '')) AS DECIMAL(10, 2)) AS purchase_amount,
        TRIM(REPLACE(acs.createddate, '"', '')) AS purchase_date,
        TRIM(REPLACE(l.state, '"', '')) AS Geographic_Transaction_State,
        ROW_NUMBER() OVER (PARTITION BY TRIM(REPLACE(c.first, '"', '')), TRIM(REPLACE(c.last, '"', '')), TRIM(REPLACE(c.email, '"', '')), l.customerid ORDER BY TRIM(REPLACE(acs.createddate, '"', '')) DESC) AS rn
    FROM
        dbo_customer_csv c
    LEFT JOIN
        dbo_lease_csv l ON c.id = l.customerid
    LEFT JOIN
        dbo_customeraddress_csv ca ON ca.customerid = l.customerid
    LEFT JOIN
        dbo_accountconfigurationstage_csv acs ON acs.customerid = l.customerid
    LEFT JOIN
        dbo_customeraddress_csv a ON l.customerid = a.customerid
)
SELECT
    first,
    last,
    email,
    StreetNumber,
    StreetName,
    City,
    State,
    Zip,
    address,
    customerid,
    creditscore,
    SUM(purchase_amount) AS total_purchase_amount,
    purchase_date,
    SUM(initpayment) AS total_initpayment,
    SUM(initpayments) AS total_initpayments,
    Geographic_Transaction_State
FROM CTE
WHERE rn = 1 OR rn IS NULL
GROUP BY
    first,
    last,
    email,
    StreetNumber,
    StreetName,
    City,
    State,
    Zip,
    address,
    customerid,
    creditscore,
    purchase_date,
    Geographic_Transaction_State
ORDER BY
    total_purchase_amount DESC
LIMIT 50000









--- 25 june avg emails

    
    
    
    
    WITH cleaned_data AS (
    SELECT 
        TRIM(REPLACE(c.first, '"', '')) AS first,
        TRIM(REPLACE(c.last, '"', '')) AS last,
        TRIM(REPLACE(c.email, '"', '')) AS email,
        from_unixtime(CAST(TRIM(REPLACE(c.ts, '"', '')) AS bigint)) AS timestamp,
        YEAR(from_unixtime(CAST(TRIM(REPLACE(c.ts, '"', '')) AS bigint))) AS year,
        MONTH(from_unixtime(CAST(TRIM(REPLACE(c.ts, '"', '')) AS bigint))) AS month
    FROM dbo_customer_csv c
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
    ae.avg_emails_per_month
    -- ,yd.total_emails_year / 12.0 AS avg_emails_per_month_from_year -- Approximate average per month based on yearly total
FROM 
    monthly_data md
JOIN 
    yearly_data yd ON md.year = yd.year
JOIN 
    average_emails ae ON md.year = ae.year
ORDER BY 
    md.year desc, md.month;



































--- 25/june






WITH CTE AS (
    SELECT
        TRIM(REPLACE(c.first, '"', '')) AS first,
        TRIM(REPLACE(c.last, '"', '')) AS last,
        TRIM(REPLACE(c.email, '"', '')) AS email,
        from_unixtime(CAST(TRIM(REPLACE(c.ts, '"', '')) AS bigint)) AS timestamp,
        YEAR(from_unixtime(CAST(TRIM(REPLACE(c.ts, '"', '')) AS bigint))) AS year,
        MONTH(from_unixtime(CAST(TRIM(REPLACE(c.ts, '"', '')) AS bigint))) AS month,
        l.customerid,
        TRIM(REPLACE(ca.streetnumber, '"', '')) as StreetNumber,
        TRIM(REPLACE(ca.streetname, '"', '')) as StreetName,
        TRIM(REPLACE(ca.city, '"', '')) as City,
        TRIM(REPLACE(ca.state, '"', '')) as State,
        TRIM(REPLACE(a.zip, '"', '')) as Zip,
        CONCAT_WS(', ',
            TRIM(REPLACE(ca.streetnumber, '"', '')),
            TRIM(REPLACE(ca.streetname, '"', '')),
            TRIM(REPLACE(ca.city, '"', '')),
            TRIM(REPLACE(ca.state, '"', ''))
        ) AS address,
        TRIM(REPLACE(c.creditscore, '"', '')) AS creditscore,
        CASE 
            WHEN TRIM(REPLACE(l.initpayment, '"', '')) = '' THEN 0
            ELSE CAST(TRIM(REPLACE(l.initpayment, '"', '')) AS DECIMAL(10, 2))
        END AS initpayment,
        CASE 
            WHEN TRIM(REPLACE(l.initpayments, '"', '')) = '' THEN 0
            ELSE CAST(TRIM(REPLACE(l.initpayments, '"', '')) AS DECIMAL(10, 2))
        END AS initpayments,
        CAST(TRIM(REPLACE(acs.amount, '"', '')) AS DECIMAL(10, 2)) AS purchase_amount,
        from_unixtime(cast(regexp_replace(entrytimestamp, '"', '') AS bigint)) AS entrytimestamp,
        TRIM(REPLACE(acs.createddate, '"', '')) AS createddate, 
        TRIM(REPLACE(l.state, '"', '')) AS Geographic_Transaction_State,
        ROW_NUMBER() OVER (PARTITION BY TRIM(REPLACE(c.first, '"', '')), TRIM(REPLACE(c.last, '"', '')), TRIM(REPLACE(c.email, '"', '')), l.customerid ORDER BY TRIM(REPLACE(acs.createddate, '"', '')) DESC) AS rn
    FROM
        dbo_customer_csv c
    LEFT JOIN
        dbo_lease_csv l ON c.id = l.customerid
    LEFT JOIN
        dbo_customeraddress_csv ca ON ca.customerid = l.customerid
    LEFT JOIN
        dbo_accountconfigurationstage_csv acs ON acs.customerid = l.customerid
    LEFT JOIN
        dbo_customeraddress_csv a ON l.customerid = a.customerid
)
SELECT
    CTE.first,
    CTE.last,
    CTE.email,
    CTE.timestamp,
    CTE.year,
    CTE.month,
    CTE.StreetNumber,
    CTE.StreetName,
    CTE.City,
    CTE.State,
    CTE.Zip,
    CTE.address,
    CTE.customerid,
    CTE.creditscore,
    SUM(CTE.purchase_amount) AS total_purchase_amount,
    CTE.entrytimestamp,
    CTE.createddate, 
    SUM(CTE.initpayment) AS total_initpayment,
    SUM(CTE.initpayments) AS total_initpayments,
    CTE.Geographic_Transaction_State,
    MAX(ars.processdate) AS processdate
FROM
    CTE
LEFT JOIN
    dbo_achreturnsstatus_csv ars ON CTE.customerid = ars.leaseid
WHERE 
    (CTE.rn = 1 OR CTE.rn IS NULL)
GROUP BY
    CTE.first,
    CTE.last,
    CTE.email,
    CTE.timestamp,
    CTE.year,
    CTE.month,
    CTE.StreetNumber,
    CTE.StreetName,
    CTE.City,
    CTE.State,
    CTE.Zip,
    CTE.address,
    CTE.customerid,
    CTE.creditscore,
    CTE.entrytimestamp,
    CTE.createddate,  
    CTE.Geographic_Transaction_State
ORDER BY
    total_purchase_amount DESC
LIMIT 50000;
