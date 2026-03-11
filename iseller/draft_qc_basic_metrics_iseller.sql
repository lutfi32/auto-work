WITH
    -- items totals
    cte_items_agg AS (
        SELECT
            RefId,
            LocalDate,
            SUM(items_SubTotal) AS Item_SubTotal,
            SUM(items_Discount) AS Item_Discount,
            SUM(items_Total) AS Item_Total,
            SUM(items_Tax) AS Item_Tax
        FROM
            `flash-coffee-prod.iseller.draft_id_fact_items_iseller`
        WHERE
            PaymentStatus = 'COMPLETED'
        GROUP BY 1, 2
    ),

    -- orders totals
    cte_orders_agg AS (
        SELECT
            RefId,
            LocalDate,
            max(paymentMethod1)paymentMethod1,
            SUM(SubTotal) AS Order_SubTotal,
            SUM(Discount) AS Order_Discount,
            SUM(Total) AS Order_Total,
            SUM(Tax) AS Order_Tax
        FROM
            `flash-coffee-prod.iseller.draft_id_fact_orders_iseller`
        WHERE
            PaymentStatus = 'COMPLETED'
        GROUP BY 1, 2
    )

-- join and compare
SELECT

    -- match status
    CASE
        WHEN (
            ABS(IFNULL(i.Item_Total, 0) - IFNULL(o.Order_Total, 0)) > 0.001
            OR ABS(IFNULL(i.Item_Tax, 0) - IFNULL(o.Order_Tax, 0)) > 0.001
            OR ABS(IFNULL(i.Item_SubTotal, 0) - IFNULL(o.Order_SubTotal, 0)) > 0.001
            OR ABS(IFNULL(i.Item_Discount, 0) - IFNULL(o.Order_Discount, 0)) > 0.001
        ) THEN '❌ Anomaly'
        ELSE '✅ Matched'
    END AS Match_Status,

    -- keys
    COALESCE(i.RefId, o.RefId) AS RefId,
    COALESCE(i.LocalDate, o.LocalDate) AS LocalDate,
    paymentMethod1,

    -- subtotal check
    ROUND(i.Item_SubTotal, 2) AS Item_SubTotal,
    ROUND(o.Order_SubTotal, 2) AS Order_SubTotal,
    ROUND(IFNULL(i.Item_SubTotal, 0) - IFNULL(o.Order_SubTotal, 0), 2) AS SubTotal_Difference,

    -- discount check
    ROUND(i.Item_Discount, 2) AS Item_Discount,
    ROUND(o.Order_Discount, 2) AS Order_Discount,
    ROUND(IFNULL(i.Item_Discount, 0) - IFNULL(o.Order_Discount, 0), 2) AS Discount_Difference,

    -- total check
    ROUND(i.Item_Total, 2) AS Item_Total,
    ROUND(o.Order_Total, 2) AS Order_Total,
    ROUND(IFNULL(i.Item_Total, 0) - IFNULL(o.Order_Total, 0), 2) AS Total_Difference,

    -- tax check
    ROUND(i.Item_Tax, 2) AS Item_Tax,
    ROUND(o.Order_Tax, 2) AS Order_Tax,
    ROUND(IFNULL(i.Item_Tax, 0) - IFNULL(o.Order_Tax, 0), 2) AS Tax_Difference,

    -- anomaly flag
    (
        ABS(IFNULL(i.Item_Total, 0) - IFNULL(o.Order_Total, 0)) > 0.001
        OR ABS(IFNULL(i.Item_Tax, 0) - IFNULL(o.Order_Tax, 0)) > 0.001
        OR ABS(IFNULL(i.Item_SubTotal, 0) - IFNULL(o.Order_SubTotal, 0)) > 0.001
        OR ABS(IFNULL(i.Item_Discount, 0) - IFNULL(o.Order_Discount, 0)) > 0.001
    ) AS Is_Anomaly,


FROM
    cte_items_agg AS i
FULL OUTER JOIN
    cte_orders_agg AS o
    ON i.RefId = o.RefId
    AND i.LocalDate = o.LocalDate
where true 
-- and paymentMethod1 like '%GoFood%'
-- and item_discount > 0
-- and o.refid = 'a9eee2c7-7d02-4512-b280-e5b97da01706'
ORDER BY
    LocalDate desc,
    RefId
-- limit 5