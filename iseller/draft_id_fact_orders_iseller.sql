WITH base AS (
  SELECT
    o.*,
    od
  FROM `flash-coffee-prod.iseller.iseller_orders` o,
  UNNEST(JSON_EXTRACT_ARRAY(o.order_details)) od
),

item_calc AS (
  SELECT
    order_id,
    COUNT(*) AS items_qty,
    SUM(
      CASE
        WHEN JSON_EXTRACT_SCALAR(od, '$.product_type') LIKE '%Food%' THEN 1
        ELSE 0
      END
    ) AS food_qty
  FROM base
  GROUP BY order_id
), 
order_promo as (
  SELECT
    order_id,
    MAX(IF(r=1,n,NULL)) promoName1, MAX(IF(r=1,a,NULL)) promoAmount1,
    MAX(IF(r=2,n,NULL)) promoName2, MAX(IF(r=2,a,NULL)) promoAmount2,
    MAX(IF(r=3,n,NULL)) promoName3, MAX(IF(r=3,a,NULL)) promoAmount3,
    SUM(a) as promoAmountTotal
    from (
    SELECT
      o.order_id,
      COALESCE(
        JSON_EXTRACT_SCALAR(x,'$.discount_name'), 
        JSON_EXTRACT_SCALAR(x,'$.promotion_name')
      ) n,
      COALESCE(
        SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.discount_amount') AS FLOAT64),
        SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.promotion_amount') AS FLOAT64),
        0
      ) a,
      ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY 1) r
    FROM `flash-coffee-prod.iseller.iseller_orders` o,
    UNNEST(ARRAY_CONCAT(
      IFNULL(JSON_EXTRACT_ARRAY(o.discounts),[]), IFNULL(JSON_EXTRACT_ARRAY(o.promotions),[])
    )) x
  )
  GROUP BY order_id
)

SELECT
  o.order_id AS refId,
  o.order_reference AS invoiceNumber,
  o.outlet_id AS storeId,
  o.customer_id AS customerRefId,
  o.customer_phone_number AS phoneNumber,

  CASE
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%grabfood%' THEN 'GrabFood - Delivery'
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%shopeefood%' THEN 'ShopeeFood - Delivery'
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%gofood%' THEN 'GoFood - Delivery' 
    ELSE JSON_EXTRACT_SCALAR(o.transactions, '$[0].payment_type_name')
  END AS paymentMethod1,
  JSON_EXTRACT_SCALAR(o.transactions, '$[1].payment_type_name') AS paymentMethod2,
  JSON_EXTRACT_SCALAR(o.transactions, '$[2].payment_type_name') AS paymentMethod3,

  SAFE_CAST(JSON_EXTRACT_SCALAR(o.transactions, '$[0].amount') AS FLOAT64) AS paymentMethod1_amount,
  SAFE_CAST(JSON_EXTRACT_SCALAR(o.transactions, '$[1].amount') AS FLOAT64) AS paymentMethod2_amount,
  SAFE_CAST(JSON_EXTRACT_SCALAR(o.transactions, '$[2].amount') AS FLOAT64) AS paymentMethod3_amount,

  CASE  
    WHEN LOWER(o.discounts) LIKE '%wastage%' THEN 'Wastage'
    WHEN LOWER(o.discounts) LIKE '%barista%meal%' THEN 'Barista Drink & Snack (100%)'
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%grabfood%' THEN 'GrabFood - Delivery'
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%shopeefood%' THEN 'ShopeeFood - Delivery'
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%gofood%' THEN 'GoFood - Delivery'
    ELSE JSON_EXTRACT_SCALAR(o.transactions, '$[0].payment_type_name')
  END AS paymentMethod,

  CASE
    WHEN LOWER(o.order_type) LIKE '%delivery%' THEN 'Delivery'
    ELSE 'Pick Up'
  END AS deliveryMethod,

  o.order_date AS createdAt,
  DATETIME(o.order_date) AS localTime,
  DATE(o.order_date) AS localDate,
  FORMAT_DATE('%Y-%m', DATE(o.order_date)) AS localMonth,
  FORMAT_DATE('%G-%V', DATE(o.order_date)) AS localWeek,

  o.total_order_amount + o.total_tax_amount + o.total_additional_final_amount AS subTotal,
  CASE 
    -- WHEN channel_detail = 'gofood' THEN op.promoAmountTotal
    WHEN channel_detail = 'gofood' THEN (o.total_discount_amount + o.total_promotion_amount) * 1.11
    ELSE o.total_discount_amount + o.total_promotion_amount
  END AS discount,
  CASE
    WHEN channel_detail = 'gofood' THEN
      o.total_order_amount 
      - COALESCE((o.total_discount_amount + o.total_promotion_amount) * 1.11,0)
      + COALESCE(total_additional_final_amount,0)
      + COALESCE(o.total_tax_amount,0)
    ELSE
      o.total_order_amount 
      - (COALESCE(o.total_discount_amount,0) + COALESCE(o.total_promotion_amount,0))
      + COALESCE(total_additional_final_amount,0)
      + COALESCE(o.total_tax_amount,0)
  END AS total,
  o.total_tax_amount AS tax,
  o.total_tax_amount AS actual_tax,

  upper(o.status) orderStatus,
  o.total_amount AS nettSales,
  -- CASE WHEN o.status = 'cancelled' THEN o.total_amount END AS refund,
  o.currency,

  CAST(NULL AS BOOL) AS useFlashPoint,
  CAST(NULL AS FLOAT64) AS flashPointAmountPaid,
  CAST(NULL AS FLOAT64) AS localCurrencyAmountPaid,

  JSON_EXTRACT_SCALAR(o.order_details, '$[0].order_detail_id') AS orderNumber,
  -- o.payment_status AS paymentStatus,
  CASE WHEN o.status = 'cancelled' THEN 'CANCELLED' ELSE 'COMPLETED' END AS paymentStatus,

  CONCAT(o.customer_first_name, ' ', o.customer_last_name) AS customerName,
  o.customer_email AS customerEmail,

  o.outlet_name AS storeName,
  o.outlet_code AS storeNameInternal,
  o.outlet_id AS sh_storeId,
  case
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%grabfood%' THEN 'Delivery'
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%shopeefood%' THEN 'Delivery'
    WHEN LOWER(concat(o.channel_detail,o.order_type)) LIKE '%gofood%' THEN 'Delivery' 
  else 'Offline' end as orderType,

  o.discounts AS coupon_applied,
  CAST(NULL AS STRING) AS coupon_accepted,

  JSON_EXTRACT_SCALAR(o.discounts, '$[0].discount_name') AS promoName1,
  JSON_EXTRACT_SCALAR(o.discounts, '$[1].discount_name') AS promoName2,
  JSON_EXTRACT_SCALAR(o.discounts, '$[2].discount_name') AS promoName3,

  SAFE_CAST(JSON_EXTRACT_SCALAR(o.discounts, '$[0].discount_amount') AS FLOAT64) AS promoValue1,
  SAFE_CAST(JSON_EXTRACT_SCALAR(o.discounts, '$[1].discount_amount') AS FLOAT64) AS promoValue2,
  SAFE_CAST(JSON_EXTRACT_SCALAR(o.discounts, '$[2].discount_amount') AS FLOAT64) AS promoValue3,

  CAST(NULL AS STRING) AS challengeName,
  CAST(NULL AS FLOAT64) AS challengeLoyaltyReward,
  CAST(NULL AS FLOAT64) AS challengePointReward,
  CAST(NULL AS FLOAT64) AS getFlashPoint,
  CAST(NULL AS FLOAT64) AS getLoyaltyPoint,
  CAST(NULL AS FLOAT64) AS ledgerLoyaltyReward,
  CAST(NULL AS FLOAT64) AS ledgerPointReward,
  CAST(NULL AS FLOAT64) AS ledgerPointIssued,
  CAST(NULL AS STRING) AS customerTier,

  o.total_amount AS totalPay,
  CAST(NULL AS FLOAT64) AS changeInCash,
  JSON_EXTRACT_SCALAR(o.transactions, '$[0].transaction_id') AS trx_numbers,

  o.order_date AS created_1,
  CAST(NULL AS TIMESTAMP) AS paid_2,
  o.closed_date AS processed_3,
  o.closed_date AS completed_4,
  CAST(NULL AS TIMESTAMP) AS picked_up_5,

  'iSeller' AS userPlatform,

  o.total_shipping_amount AS deliveryFee,
  CAST(NULL AS FLOAT64) AS smallOrderFee,
  CAST(NULL AS FLOAT64) AS deliveryDiscountFee,
  CAST(NULL AS FLOAT64) AS cleanhubDonationAmount,

  CAST(NULL AS TIMESTAMP) AS estimated_pickup_time,
  CAST(NULL AS STRING) AS appVersionName,
  CAST(NULL AS STRING) AS appVersionCode,
  CAST(NULL AS STRING) AS financestoreid,
  CAST(NULL AS FLOAT64) AS lat,
  CAST(NULL AS FLOAT64) AS long,
  CAST(NULL AS FLOAT64) AS flashValuePaid,
  CAST(NULL AS FLOAT64) AS packagingFee,
  CAST(NULL AS FLOAT64) AS pointBefore,
  CAST(NULL AS FLOAT64) AS pointAfter,

  ic.items_qty,
  CAST(NULL AS INT64) AS vip_and_barista_qty,
  ic.food_qty

FROM `flash-coffee-prod.iseller.iseller_orders` o
LEFT JOIN item_calc ic
  ON o.order_id = ic.order_id
  -- where o.order_id = '1dfdbec8-62f1-4fdd-87b9-e5ea35b7a71f'
  left join order_promo op on op.order_id = o.order_id
ORDER BY o.order_date DESC;