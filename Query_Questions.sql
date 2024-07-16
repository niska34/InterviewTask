--1) Which user spent the most money on products on all Fridays?

SELECT TOP 1
    u.user_id,
    u.user_name,
    u.city,
    SUM(p.price * op.quantity) AS total_spent
FROM 
    Users AS u 
JOIN 
    Orders AS o ON u.user_id = o.user_id 
JOIN 
    Order_Products AS op ON o.order_id = op.order_id
JOIN
    Products AS p ON op.product_id = p.product_id
WHERE 
    DATEPART(dw, o.created) = 6  -- Assuming Sunday is 1, Friday is 6
GROUP BY 
    u.user_id, u.user_name, u.city
ORDER BY 
    total_spent DESC



--2) What are the best 3 products in each location of a user based on quantity?
;WITH BestProducts AS (
    SELECT
        u.user_name,
        u.city,
        p.product_id,
        p.product_name,
        SUM(op.quantity) AS total_quantity,
        ROW_NUMBER() OVER (PARTITION BY u.city ORDER BY SUM(op.quantity) DESC) AS rank
    FROM 
        Users AS u
    JOIN 
        Orders AS o ON u.user_id = o.user_id
    JOIN 
        Order_Products AS op ON o.order_id = op.order_id
    JOIN
        Products AS p ON op.product_id = p.product_id
    GROUP BY
        u.user_name,u.city, p.product_id, p.product_name
)
SELECT
    user_name,
    city,
    product_id,
    product_name,
    total_quantity
FROM
    BestProducts
WHERE
    rank <= 3
ORDER BY
    city,
    rank;