-- co -> consumber -> customer_id
-- SELECT * FROM "dbo_consumers_json";
-- cusp maybe hotel --amenitis
-- SELECT * FROM dbo_customer_spaces_json                                   ;
-- nob -> hotel employee email and email sent
-- SELECT * FROM dbo_notification_out_boxes_json
-- customer -> cu_id , checkin, checkup, space_type, unitlength, unit width
-- SELECT * FROM dbo_online_no_availabilities_json
-- purchase idtem, cu-id
-- SELECT * FROM dbo_order_item_folio_lines_json
-- all website order details, only useful file available,
-- SELECT * FROM dbo_order_logs_json
-- order notes associated with dbo_order_logs
-- SELECT * FROM dbo_order_notes_json
-- consumber checkin and checkout date
-- SELECT * FROM dbo_orders_json



SELECT
    json_extract_scalar(orl_xml, '$.consumer_id') AS CustomerID,
    json_extract_scalar(orl_xml, '$.firstname') AS CustomerFirsrtName,
    json_extract_scalar(orl_xml, '$.lastname') AS CustomeLastName,
    json_extract_scalar(orl_xml, '$.phone') AS CellPhone,
    json_extract_scalar(orl_xml, '$.email') AS Email,
    json_extract_scalar(orl_xml, '$.state') AS State,
    json_extract_scalar(orl_xml, '$.country') AS Country,
    json_extract_scalar(orl_xml, '$.city') AS City,
    json_extract_scalar(orl_xml, '$.zipcode') AS zipcode,
    json_extract_scalar(orl_xml, '$.checkindate') AS CheckinDate,
    json_extract_scalar(orl_xml, '$.checkoutdate') AS CheckoutDate,
    DATE_DIFF('day',
        CAST(REPLACE(json_extract_scalar(orl_xml, '$.checkindate'), 'T', ' ') AS TIMESTAMP),
        CAST(REPLACE(json_extract_scalar(orl_xml, '$.checkoutdate'), 'T', ' ') AS TIMESTAMP)
    ) +1 AS DurationOfStay,
    json_extract_scalar(orl_xml, '$.space_id') AS SpaceID,
    json_extract_scalar(orl_xml, '$.space_type') AS SpaceType,
    json_extract_scalar(orl_xml, '$.space_num') AS SpaceNum,
    json_extract_scalar(orl_xml, '$.address1') AS SpaceAddress1,
    json_extract_scalar(orl_xml, '$.address2') AS SpaceAddress2,
    json_extract_scalar(orl_xml, '$.space_amount') AS CostofVacation,
    json_extract_scalar(orl_xml, '$.payment_date') AS BookingDate,
    json_extract_scalar(orl_xml, '$.payment_type') AS PaymentType,
    json_extract_scalar(orl_xml, '$.payment_cardnumber') AS PaymentCardNumber,
    json_extract_scalar(orl_xml, '$.unittype') AS UnitType,
    json_extract_scalar(orl_xml, '$.unitlength') AS UnitLength,
    json_extract_scalar(orl_xml, '$.unitwidth') AS UnitWidth,
    json_extract_scalar(orl_xml, '$.ampservice') AS ampservice
FROM
    dbo_order_logs_json;








    
    
    
SELECT DISTINCT
    c.co_id,
    c.co_first_name,
    c.co_last_name,
    c.co_address1,
    c.co_city,
    c.co_phone_home,
    c.co_e_mail,
    json_extract_scalar(olj.orl_xml, '$.checkindate') AS CheckinDate,
    json_extract_scalar(olj.orl_xml, '$.checkoutdate') AS CheckoutDate,
    DATE_DIFF('day',
        CAST(REPLACE(json_extract_scalar(olj.orl_xml, '$.checkindate'), 'T', ' ') AS TIMESTAMP),
        CAST(REPLACE(json_extract_scalar(olj.orl_xml, '$.checkoutdate'), 'T', ' ') AS TIMESTAMP)
    ) +1 AS DurationOfStay,
    json_extract_scalar(olj.orl_xml, '$.space_id') AS SpaceID,
    json_extract_scalar(olj.orl_xml, '$.space_type') AS SpaceType,
    json_extract_scalar(olj.orl_xml, '$.space_num') AS SpaceNum,
    json_extract_scalar(olj.orl_xml, '$.address1') AS SpaceAddress1,
    json_extract_scalar(olj.orl_xml, '$.address2') AS SpaceAddress2,
    json_extract_scalar(olj.orl_xml, '$.space_amount') AS CostofVacation,
    json_extract_scalar(olj.orl_xml, '$.payment_date') AS BookingDate,
    json_extract_scalar(olj.orl_xml, '$.payment_type') AS PaymentType,
    json_extract_scalar(olj.orl_xml, '$.payment_cardnumber') AS PaymentCardNumber,
    json_extract_scalar(olj.orl_xml, '$.unittype') AS UnitType,
    json_extract_scalar(olj.orl_xml, '$.unitlength') AS UnitLength,
    json_extract_scalar(olj.orl_xml, '$.unitwidth') AS UnitWidth,
    json_extract_scalar(olj.orl_xml, '$.ampservice') AS ampservice
FROM 
    dbo_consumers_json c
LEFT JOIN 
    dbo_order_logs_json olj ON c.co_id = CAST(json_extract_scalar(olj.orl_xml, '$.consumer_id') AS INT)
WHERE 
    c.co_e_mail != '';










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
    dbo_consumers_json c
LEFT JOIN 
    dbo_consumer_vehicles_json v ON c.co_id = v.co_id
LEFT JOIN 
    dbo_auto_sys_notes_json n ON c.co_id = n.co_id
LEFT JOIN 
    dbo_consumer_favorite_destinations_json f ON c.co_id = f.co_id
LEFT JOIN 
    dbo_consumer_favorite_spaces_json s ON c.co_id = s.co_id
LEFT JOIN 
    dbo_consumer_gift_cards_json g ON c.co_id = g.co_id
LEFT JOIN 
    dbo_customer_consumer_credit_cards_json cr ON c.co_id = cr.co_id
LEFT JOIN 
    dbo_customer_consumers_json cu ON c.co_id = cu.co_id
LEFT JOIN 
    dbo_order_groups_json og ON c.co_id = og.co_id
LEFT JOIN 
    dbo_order_item_fulfillments_json ofl ON c.co_id = ofl.co_id
LEFT JOIN 
    dbo_order_logs_json olj ON c.co_id = CAST(json_extract_scalar(olj.orl_xml, '$.consumer_id') AS INT)
where c.co_e_mail != ''








---14 june updated:
SELECT DISTINCT
    c.co_id,
    c.co_first_name,
    c.co_last_name,
    c.co_address1,
    c.co_city,
    c.co_phone_home,
    c.postal_code,
    c.co_phone_home,
    c.co_phone_business, 
    c.co_e_mail,
    c.co_phone_mobile, 
        json_extract_scalar(olj.orl_xml, '$.checkindate') AS CheckinDate,
    json_extract_scalar(olj.orl_xml, '$.checkoutdate') AS CheckoutDate,
    DATE_DIFF('day',
        CAST(REPLACE(json_extract_scalar(olj.orl_xml, '$.checkindate'), 'T', ' ') AS TIMESTAMP),
        CAST(REPLACE(json_extract_scalar(olj.orl_xml, '$.checkoutdate'), 'T', ' ') AS TIMESTAMP)
    ) +1 AS DurationOfStay,
    json_extract_scalar(olj.orl_xml, '$.space_id') AS SpaceID,
    json_extract_scalar(olj.orl_xml, '$.space_type') AS SpaceType,
    json_extract_scalar(olj.orl_xml, '$.space_num') AS SpaceNum,
    json_extract_scalar(olj.orl_xml, '$.address1') AS SpaceAddress1,
    json_extract_scalar(olj.orl_xml, '$.address2') AS SpaceAddress2,
    json_extract_scalar(olj.orl_xml, '$.space_amount') AS CostofVacation,
    json_extract_scalar(olj.orl_xml, '$.payment_date') AS BookingDate,
    json_extract_scalar(olj.orl_xml, '$.payment_type') AS PaymentType,
    json_extract_scalar(olj.orl_xml, '$.payment_cardnumber') AS PaymentCardNumber,
    json_extract_scalar(olj.orl_xml, '$.unittype') AS UnitType,
    json_extract_scalar(olj.orl_xml, '$.unitlength') AS UnitLength,
    json_extract_scalar(olj.orl_xml, '$.unitwidth') AS UnitWidth,
    json_extract_scalar(olj.orl_xml, '$.ampservice') AS ampservice
FROM 
    dbo_consumers_json c
LEFT JOIN 
    dbo_consumer_vehicles_json v ON c.co_id = v.co_id
LEFT JOIN 
    dbo_auto_sys_notes_json n ON c.co_id = n.co_id
LEFT JOIN 
    dbo_consumer_favorite_destinations_json f ON c.co_id = f.co_id
LEFT JOIN 
    dbo_consumer_favorite_spaces_json s ON c.co_id = s.co_id
LEFT JOIN 
    dbo_consumer_gift_cards_json g ON c.co_id = g.co_id
LEFT JOIN 
    dbo_customer_consumer_credit_cards_json cr ON c.co_id = cr.co_id
LEFT JOIN 
    dbo_customer_consumers_json cu ON c.co_id = cu.co_id
LEFT JOIN 
    dbo_order_groups_json og ON c.co_id = og.co_id
LEFT JOIN 
    dbo_order_item_fulfillments_json ofl ON c.co_id = ofl.co_id
LEFT JOIN 
    dbo_order_logs_json olj ON c.co_id = CAST(json_extract_scalar(olj.orl_xml, '$.consumer_id') AS INT)
WHERE 
    c.co_e_mail != '';