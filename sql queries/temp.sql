SELECT DISTINCT
    u.email,
    u.full_name,
    u.username,
    u.phone,
    u.gender,
    u.dateofbirth,
    u.firstordermadeat,
    u.lastordermadeat,
    u.totalorderplaced,
    ul.name AS address_type,
    CONCAT_WS(', ', ul.address, ul.number, ul.town, ul.zip) AS full_address,
    cj.status AS order_status,
    cj.total AS amount_spent
FROM
    user_json u
JOIN
    userlocation_json ul ON u._id = REPLACE(ul._p_user, '_User$', '')
JOIN
    cart_json cj ON u._id = REPLACE(cj._p_user, '_User$', '')
WHERE
    u.dateofbirth != ''
    AND u.gender != ''
LIMIT 2000;
