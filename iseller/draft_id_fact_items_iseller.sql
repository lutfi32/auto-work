WITH iseller_order_details as (
select
  TO_JSON_STRING(ARRAY(
    SELECT AS STRUCT
      COALESCE(JSON_EXTRACT_SCALAR(x,'$.discount_name'),
               JSON_EXTRACT_SCALAR(x,'$.promotion_name')) promo_name,
      COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.discount_amount') AS FLOAT64),
               SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.promotion_amount') AS FLOAT64),0) promo_amount
    FROM UNNEST(ARRAY_CONCAT(IFNULL(JSON_EXTRACT_ARRAY(od.discounts),[]),
                             IFNULL(JSON_EXTRACT_ARRAY(od.promotions),[]))) x
    WHERE COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.discount_amount') AS FLOAT64),
                   SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.promotion_amount') AS FLOAT64),0) > 0
    ORDER BY promo_amount DESC
  )) promos,
  --
  coalesce(safe_divide(additional_charge , sum(additional_charge) over (partition by od.order_id)) 
        * total_additional_final_amount ,0) as additional_charge,
  coalesce(safe_divide(tax_amount , sum(tax_amount) over (partition by od.order_id)) 
        * total_tax_amount ,0) as tax_amount,
  --
  od.* except (additional_charge, tax_amount),
  FROM `flash-coffee-prod.iseller.iseller_order_details`  od
  left join `flash-coffee-prod.iseller.iseller_orders`  o  on od.order_id = o.order_id
), 

products AS (
  SELECT
    order_id, order_detail_id,
    sku, product_id, product_name, product_generic_name,
    total_order_amount, subtotal, additional_charge,
    quantity, base_price, tax_amount, tax_percentage,
    discount_amount, -- discount yang di dapat di item level
    discount_order_amount, -- discount yg di prorate di order level
    promotions, discounts, -- JSON OBJECT
    case 
      when product_name like '%Buy 1%Get 1%' then True
      when product_name like '%Solo Meal%+%' then True
      when sku like 'BND%' then True
      else False end is_product_bundles,

    -- coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promos, '$[0].promo_amount') AS FLOAT64),0) disc_promo_amount_1,
    -- coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promos, '$[1].promo_amount') AS FLOAT64),0) disc_promo_amount_2,
    -- coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promos, '$[2].promo_amount') AS FLOAT64),0) disc_promo_amount_3,
    -- IFNULL((SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.promo_amount') AS FLOAT64))
    --       FROM UNNEST(JSON_EXTRACT_ARRAY(promos)) x),0) disc_promo_amount_total,
    
    coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(discounts, '$[0].discount_amount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promotions, '$[0].promotion_amount') AS FLOAT64),0)
        disc_promo_amount_1,
    coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(discounts, '$[1].discount_amount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promotions, '$[1].promotion_amount') AS FLOAT64),0)
        disc_promo_amount_2,
    coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(discounts, '$[2].discount_famount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promotions, '$[2].promotion_amount') AS FLOAT64),0)
        disc_promo_amount_3,
    IFNULL((SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.discount_amount') AS FLOAT64))
          FROM UNNEST(JSON_EXTRACT_ARRAY(discounts)) x),0) +
    IFNULL((SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.promotion_amount') AS FLOAT64))
            FROM UNNEST(JSON_EXTRACT_ARRAY(promotions)) x),0) AS disc_promo_amount_total
  FROM `iseller_order_details`
  WHERE product_modifier_id IS NULL
), 

product_bundles AS (
  select * from products where is_product_bundles is True
),
product_non_bundles AS (
  select * from products where is_product_bundles is False
),

modifiers AS (
  SELECT
    order_id, parent_order_detail_id, sku,
    string_agg(distinct product_name) as mod_product_name,
    string_agg(distinct modifier_product_sku) as mod_modifier_product_sku,
    SUM(total_order_amount) AS mod_total_order_amount,
    SUM(subtotal) AS mod_subtotal, SUM(additional_charge) AS mod_additional_charge,
    sum(discount_amount) as mod_discount_amount, sum(discount_order_amount) as mod_discount_order_amount,
    SUM(tax_amount) AS mod_tax_amount,

    -- sum(coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promos, '$[0].promo_amount') AS FLOAT64),0)) mod_disc_promo_amount_1,
    -- sum(coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promos, '$[1].promo_amount') AS FLOAT64),0)) mod_disc_promo_amount_2,
    -- sum(coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promos, '$[2].promo_amount') AS FLOAT64),0)) mod_disc_promo_amount_3,
    -- sum(IFNULL((SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.promo_amount') AS FLOAT64))
    --       FROM UNNEST(JSON_EXTRACT_ARRAY(promos)) x),0)) mod_disc_promo_amount_total,

    sum(coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(discounts, '$[0].discount_amount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promotions, '$[0].promotion_amount') AS FLOAT64),0)) mod_disc_promo_amount_1,
    sum(coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(discounts, '$[1].discount_amount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promotions, '$[1].promotion_amount') AS FLOAT64),0)) mod_disc_promo_amount_2,
    sum(coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(discounts, '$[2].discount_amount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(promotions, '$[2].promotion_amount') AS FLOAT64),0)) mod_disc_promo_amount_3,
    sum(IFNULL((SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.discount_amount') AS FLOAT64))
          FROM UNNEST(JSON_EXTRACT_ARRAY(discounts)) x),0) +
    IFNULL((SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.promotion_amount') AS FLOAT64))
            FROM UNNEST(JSON_EXTRACT_ARRAY(promotions)) x),0)) AS mod_disc_promo_amount_total
  FROM `iseller_order_details`
  WHERE product_modifier_id IS NOT NULL
  GROUP BY 1, 2, 3
),

modifier_bundles AS (
  SELECT
    d.order_id, d.parent_order_detail_id, d.sku, d.modifier_product_sku,
    d.product_name, d.product_generic_name, 
    d.total_order_amount, d.subtotal, d.additional_charge, 
    d.quantity, d.base_price, d.tax_amount, d.tax_percentage, 
    d.discount_amount, d.discount_order_amount, d.promotions, d.discounts,
    --
    d.quantity / sum(d.quantity) over (partition by parent_order_detail_id) bnd_factor,
    --
    coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(d.discounts, '$[0].discount_amount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(d.promotions, '$[0].promotion_amount') AS FLOAT64),0) disc_promo_amount_1,
    coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(d.discounts, '$[1].discount_amount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(d.promotions, '$[1].promotion_amount') AS FLOAT64),0) disc_promo_amount_2,
    coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(d.discounts, '$[2].discount_amount') AS FLOAT64),0)
      +coalesce(SAFE_CAST(JSON_EXTRACT_SCALAR(d.promotions, '$[2].promotion_amount') AS FLOAT64),0) disc_promo_amount_3,
    IFNULL((SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.discount_amount') AS FLOAT64))
          FROM UNNEST(JSON_EXTRACT_ARRAY(d.discounts)) x),0) +
    IFNULL((SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(x,'$.promotion_amount') AS FLOAT64))
            FROM UNNEST(JSON_EXTRACT_ARRAY(d.promotions)) x),0) AS disc_promo_amount_total
  FROM `iseller_order_details` d
  join product_bundles pb on pb.order_detail_id = d.parent_order_detail_id
),

-- selects non-modifier (base) item lines
base_items AS (
  SELECT
    d.order_id, d.product_id, null as bundle_product_id,
    d.product_name, d.product_generic_name, m.mod_product_name, 
    m.mod_modifier_product_sku, d.sku, d.quantity,
    -- Combined financial totals 
    COALESCE(d.subtotal, 0) + COALESCE(m.mod_subtotal, 0) AS subtotal,
    COALESCE(d.total_order_amount, 0) + COALESCE(m.mod_total_order_amount, 0) AS total_order_amount,
    COALESCE(d.tax_amount, 0) + COALESCE(m.mod_tax_amount, 0) AS tax_amount,
    COALESCE(d.additional_charge, 0) + COALESCE(m.mod_additional_charge, 0) AS additional_charge,
    COALESCE(d.discount_amount, 0) 
      + COALESCE(m.mod_discount_amount, 0) AS discount_amount,
    COALESCE(d.discount_order_amount, 0) 
      + COALESCE(m.mod_discount_order_amount, 0) AS discount_order_amount,
    COALESCE(d.disc_promo_amount_1, 0) + COALESCE(m.mod_disc_promo_amount_1, 0) AS disc_promo_amount_1,
    COALESCE(d.disc_promo_amount_2, 0) + COALESCE(m.mod_disc_promo_amount_2, 0) AS disc_promo_amount_2,
    COALESCE(d.disc_promo_amount_3, 0) + COALESCE(m.mod_disc_promo_amount_3, 0) AS disc_promo_amount_3,
    COALESCE(d.disc_promo_amount_total, 0) + COALESCE(m.mod_disc_promo_amount_total, 0) disc_promo_amount_total,
    d.tax_percentage, d.base_price,
    d.promotions AS i_promotions, d.discounts AS i_discounts
  FROM product_non_bundles d
  LEFT JOIN modifiers m ON d.order_id = m.order_id AND d.sku = m.sku AND d.order_detail_id = m.parent_order_detail_id

  union all 

  SELECT
    d.order_id, d.product_id, product_id as bundle_product_id,
    coalesce(m.product_name,d.product_name) product_name,
    d.product_generic_name, 
    d.product_name, 
    m.modifier_product_sku, 
    coalesce(m.modifier_product_sku,d.sku) sku,
    coalesce(m.quantity, d.quantity) quantity,
    -- Combined financial totals 
    COALESCE(m.subtotal, 0) + COALESCE(bnd_factor * d.subtotal, 0) AS subtotal,
    COALESCE(m.total_order_amount, 0) + COALESCE(bnd_factor * d.total_order_amount, 0) AS total_order_amount,
    COALESCE(m.tax_amount, 0) + COALESCE(bnd_factor * d.tax_amount, 0) AS tax_amount,
    COALESCE(m.additional_charge, 0) + COALESCE(bnd_factor * d.additional_charge, 0) AS additional_charge,
    COALESCE(m.discount_amount, 0) + COALESCE(bnd_factor * d.discount_amount, 0) AS discount_amount,
    COALESCE(m.discount_order_amount, 0) + COALESCE(bnd_factor * d.discount_order_amount, 0) AS discount_order_amount,
    COALESCE(m.disc_promo_amount_1, 0) + COALESCE(bnd_factor * d.disc_promo_amount_1, 0) AS disc_promo_amount_1,
    COALESCE(m.disc_promo_amount_2, 0) + COALESCE(bnd_factor * d.disc_promo_amount_2, 0) AS disc_promo_amount_2,
    COALESCE(m.disc_promo_amount_3, 0) + COALESCE(bnd_factor * d.disc_promo_amount_3, 0) AS disc_promo_amount_3,
    COALESCE(m.disc_promo_amount_total, 0) + COALESCE(bnd_factor * d.disc_promo_amount_total, 0) AS disc_promo_amount_total,
    d.tax_percentage, d.base_price,
    -- d.promotions AS i_promotions,   
    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT
        CASE WHEN JSON_EXTRACT_SCALAR(p,'$.promotion_name')='GoFood Promotion'
            THEN d.product_name
            ELSE JSON_EXTRACT_SCALAR(p,'$.promotion_name') END AS promotion_name,
        JSON_EXTRACT_SCALAR(p,'$.promotion_type') AS promotion_type,
        SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(p,'$.promotion_amount') AS FLOAT64)) AS promotion_amount
      FROM UNNEST(IFNULL(JSON_EXTRACT_ARRAY(d.promotions),[])) p
      GROUP BY 1,2
    )) AS i_promotions,
    d.discounts AS i_discounts
  FROM product_bundles d
  LEFT JOIN modifier_bundles m ON d.order_detail_id = m.parent_order_detail_id
),
 
base_items_with_bag as (
  select order_id, product_id, product_name,product_generic_name,mod_product_name, sku, quantity,
  subtotal, total_order_amount,
  tax_amount,0 tax_amount_include,
  additional_charge,discount_amount,discount_order_amount,
  disc_promo_amount_total,
  disc_promo_amount_total - (disc_promo_amount_total / (1 + tax_percentage/100)) tax_disc_promo,
  disc_promo_amount_1,disc_promo_amount_2,disc_promo_amount_3,tax_percentage,base_price,
  i_promotions,i_discounts,
  mod_modifier_product_sku, bundle_product_id
  from base_items

  union all 
  select order_id, null product_id, 'Small Paper Bag' product_name, null product_generic_name, 
  null mod_product_name, 'OTH0019' sku, 1 quantity,
  sum(additional_charge) subtotal, 
  sum(additional_charge) total_order_amount,
  0 tax_amount, 
  sum(additional_charge) *  max(tax_percentage/100) tax_amount_include, 
  null additional_charge, 0 discount_amount, 0 discount_order_amount,
  0 disc_promo_amount_total,0 tax_disc_promo,
  null disc_promo_amount_1, null disc_promo_amount_2, null disc_promo_amount_3, 
  max(tax_percentage) tax_percentage, sum(additional_charge) base_price, null i_promotions, null i_discounts,
  null mod_modifier_product_sku, null bundle_product_id
  from base_items
  group by 1
),

order_promo_gofood as (
  select
  order_id,
  to_json_string(array_agg(struct(name, total_amount))) gofood_promo
  from (
    select
      order_id,
      coalesce(json_value(x,'$.promotion_name'),
              json_value(x,'$.discount_name')) name,
      sum(coalesce(safe_cast(json_value(x,'$.promotion_amount') as float64),
                  safe_cast(json_value(x,'$.discount_amount') as float64),0)) total_amount
    from base_items,
    unnest(array_concat(ifnull(json_query_array(i_promotions),[]),
                        ifnull(json_query_array(i_discounts),[]))) x
    group by order_id, name
  )
  group by order_id
),
order_promo as (
  SELECT a.* 
  except (promoName1,promoName2,promoName3),
  coalesce(json_extract_scalar(b.gofood_promo, '$[0].name'),promoName1) promoName1,
  coalesce(json_extract_scalar(b.gofood_promo, '$[1].name'),promoName2) promoName2,
  coalesce(json_extract_scalar(b.gofood_promo, '$[2].name'),promoName2) promoName3,
  FROM (
    SELECT
    order_id,
    MAX(channel='food_delivery' AND channel_detail='gofood') gofood_flag,
    MAX(IF(r=1,n,NULL)) promoName1, MAX(IF(r=1,a,NULL)) promoAmount1,
    MAX(IF(r=2,n,NULL)) promoName2, MAX(IF(r=2,a,NULL)) promoAmount2,
    MAX(IF(r=3,n,NULL)) promoName3, MAX(IF(r=3,a,NULL)) promoAmount3,
    SUM(a) promoAmountTotal
    FROM (
      SELECT
        o.order_id,
        COALESCE(JSON_VALUE(x,'$.discount_name'),JSON_VALUE(x,'$.promotion_name')) n,
        COALESCE(SAFE_CAST(JSON_VALUE(x,'$.discount_amount') AS FLOAT64),
                SAFE_CAST(JSON_VALUE(x,'$.promotion_amount') AS FLOAT64),0) a,
        ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY 1) r,
        channel,channel_detail
      FROM `flash-coffee-prod.iseller.iseller_orders` o,
      UNNEST(ARRAY_CONCAT(IFNULL(JSON_QUERY_ARRAY(o.discounts),[]),
                          IFNULL(JSON_QUERY_ARRAY(o.promotions),[]))) x
    )
    GROUP BY order_id
  ) a left join order_promo_gofood b on a.order_id = b.order_id and a.gofood_flag = True
),

-- joins all data and derives final columns
IsellerFact AS (
  SELECT
    -- product_name,
    -- mod_product_name,
    -- sku,
    -- mod_modifier_product_sku,

    -- core
    o.order_id AS refId,
    o.order_reference AS invoiceNumber,
    o.outlet_id AS storeId,
    o.customer_id AS customerRefId,
    o.customer_phone_number AS phoneNumber,
    -- payment methods
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
    -- delivery
    CASE
      WHEN LOWER(o.order_type) LIKE '%delivery%' THEN 'Delivery'
      ELSE 'Pick Up'
    END AS deliveryMethod,
    -- time fields
    o.order_date AS createdAt,
    DATETIME(o.order_date) AS localTime,
    DATE(o.order_date) AS localDate,
    FORMAT_DATE('%Y-%m', DATE(o.order_date)) AS localMonth,
    FORMAT_DATE('%G-%V', DATE(o.order_date)) AS localWeek,
    -- financial summary
    o.total_order_amount + o.total_tax_amount + o.total_additional_final_amount AS subTotal,
    -- o.total_discount_amount + o.total_promotion_amount AS discount,
    -- o.total_order_amount - (o.total_discount_amount + o.total_promotion_amount) + total_additional_final_amount +  o.total_tax_amount AS total,

  CASE 
    WHEN channel_detail = 'gofood' THEN op.promoAmountTotal
    ELSE o.total_discount_amount + o.total_promotion_amount
  END AS discount,
  CASE
    WHEN channel_detail = 'gofood' THEN
      o.total_order_amount 
      - COALESCE(op.promoAmountTotal,0)
      + COALESCE(total_additional_final_amount,0)
      + COALESCE(o.total_tax_amount,0)
    ELSE
      o.total_order_amount 
      - (COALESCE(o.total_discount_amount,0) + COALESCE(o.total_promotion_amount,0))
      + COALESCE(total_additional_final_amount,0)
      + COALESCE(o.total_tax_amount,0)
  END AS total,

    
  o.total_tax_amount AS tax,
    upper(o.status) orderStatus,
    CAST(o.subtotal AS FLOAT64) AS nettSales,
    0.0 AS refund,
    'IDR' AS currency,
    'N' AS useFlashPoint,
    0.0 AS flashPointAmountPaid,
    o.subtotal AS localCurrencyAmountPaid,
    o.order_reference AS orderNumber,
   CASE 
  WHEN LOWER(o.status) = 'cancelled'
       OR UPPER(o.payment_status) = 'REFUNDED'
  THEN 'CANCELLED'
  ELSE 'COMPLETED'
END AS paymentStatus,
    -- customer/store info
    CAST(NULL AS STRING) AS customerName,
    o.customer_email AS customerEmail,
    o.outlet_name AS storeName,
    o.outlet_name AS storeNameInternal,
    outlet_id AS sh_storeId,
    -- order type
    case
    when LOWER(o.order_type) like '%grabfood%' or LOWER(o.channel_detail) like '%grabfood%' then 'Delivery'
    when LOWER(o.order_type) like '%shopeefood%' or LOWER(o.channel_detail) like '%shopeefood%' then 'Delivery'
    when LOWER(o.order_type) like '%gofood%' or LOWER(o.channel_detail) like '%gofood%'  then 'Delivery'
    else 'Offline' end as orderType,
    -- order promotions
    -- JSON_EXTRACT_SCALAR(o.promotions, '$[0].promotion_name') AS promoName1,
    -- JSON_EXTRACT_SCALAR(o.promotions, '$[1].promotion_name') AS promoName2,
    -- JSON_EXTRACT_SCALAR(o.promotions, '$[2].promotion_name') AS promoName3,
    op.promoname1 AS promoName1,
    op.promoname2 AS promoName2,
    op.promoname3 AS promoName3,

    op.promoamount1 AS promoValue1,
    op.promoamount2 AS promoValue2,
    op.promoamount3 AS promoValue3,
    -- product identifiers
    d.product_id AS storehub_productId,
    d.product_id AS flashapp_productId,
    LEFT(d.sku, 7) AS storehubSKU,
    product_name AS flashAppProductName,
    product_name AS globalProductName,
    LEFT(d.sku, 7) AS sic,
    -- product category
    CAST(NULL AS STRING) AS sic_category_1,
    CAST(NULL AS STRING) AS sic_category_2,
    CAST(NULL AS STRING) AS sic_category_3,
    -- item core details
    CAST(NULL AS BOOL) AS cupitem,
    d.quantity AS items_quantity,
    -- CAST(NULL AS INT64) AS vip_and_barista_qty, 
    
--tax_disc_promo
--+ d.tax_amount_include

    d.total_order_amount - disc_promo_amount_total - d.tax_amount AS items_total,
    -- d.total_order_amount - disc_promo_amount_total AS items_total,
    d.total_order_amount + d.tax_amount AS items_subTotal, -- + d.tax_amount
    d.tax_amount AS items_tax,
    CAST(d.tax_percentage AS STRING) AS items_taxCode,
    disc_promo_amount_total items_discount,
    d.base_price AS items_unitPrice,
    -- item promotions
    COALESCE(JSON_EXTRACT_SCALAR(i_discounts, '$[0].discount_name'), 
      JSON_EXTRACT_SCALAR(i_promotions, '$[0].promotion_name')) AS items_promoName1,
    COALESCE(JSON_EXTRACT_SCALAR(i_discounts, '$[1].discount_name'), 
      JSON_EXTRACT_SCALAR(i_promotions, '$[1].promotion_name')) AS items_promoName2,
    COALESCE(JSON_EXTRACT_SCALAR(i_discounts, '$[2].discount_name'), 
      JSON_EXTRACT_SCALAR(i_promotions, '$[2].promotion_name')) AS items_promoName3,
    disc_promo_amount_1 items_promoValue1,
    disc_promo_amount_2 items_promoValue2,
    disc_promo_amount_3 items_promoValue3,
    -- processing time
    DATETIME(o.order_date) AS created_1,
    CAST(NULL AS DATETIME) AS paid_2,
    DATETIME(o.closed_date) AS processed_3,
    DATETIME(o.closed_date) AS completed_4,
    CAST(NULL AS DATETIME) AS picked_up_5,
    -- platform
    'iSeller' AS userPlatform,
    -- variants
    COALESCE(
        case when d.mod_product_name like '%ICED/%' then 'Iced' end,
        INITCAP(REGEXP_EXTRACT(LOWER(d.sku), r'-(iced|hot)-')),
        INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(iced|hot|whole bean|fine grind)')
        )
    ) AS variantAvailablein,
    INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(without ice|less ice|regular ice|extra ice|separate ice|ice)')) AS variantIce,
    COALESCE( CASE 
        WHEN LOWER(d.sku) LIKE '%-s' THEN 'Small' 
        WHEN LOWER(d.sku) LIKE '%-r' THEN 'Regular' 
        WHEN LOWER(d.sku) LIKE '%-l' THEN 'Large' END,
        INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(small|regular|large)'))
    ) AS variantSize,
    INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), 
      r'(fresh milk|oat milk|almond milk|soy milk|coconut milk|nonfat milk)')) AS variantMilkType,
    COALESCE(
        CASE WHEN LOWER(d.mod_product_name) LIKE '%less sweet%' THEN 'Less Sugar' WHEN LOWER(d.mod_product_name) LIKE '%normal sweet%' THEN 'Normal Sugar' WHEN LOWER(d.mod_product_name) LIKE '%extra sweet%' THEN 'More Sugar' END,
        INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(less sugar|normal sugar|more sugar|no sugar)'))
    ) AS variantSugarLevel,
    INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(vanilla syrup|caramel syrup|honey syrup|aren syrup|butterscotch syrup)')) AS variantExtraSyrup,
    INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(1 shot|2 shots|extra shot)')) AS variantExtraShot,
    INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(oreo|whipped cream|caramel sauce|boba)')) AS variantTopping,
    INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(arabica|robusta|blend)')) AS variantBean,
    INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(salted foam|cheese foam|seasalt foam)')) AS variantFoam,
    INITCAP(REGEXP_EXTRACT(LOWER(d.mod_product_name), r'(vanilla|caramel|honey|no flavour)')) AS variantFlavour,
    -- fees/extra
    CAST(o.total_shipping_amount AS FLOAT64) AS items_deliveryFee,
    CAST(NULL AS FLOAT64) AS items_smallOrderFee,
    CAST(NULL AS FLOAT64) AS items_deliveryDiscountFee,
    CAST(NULL AS FLOAT64) AS items_orderAmount,
    CAST(NULL AS DATETIME) AS estimated_pickup_time,
    CAST(NULL AS STRING) AS appVersionName,
    CAST(NULL AS STRING) AS appVersionCode,
    CAST(NULL AS STRING) AS financestoreid,
    CAST(NULL AS FLOAT64) AS lat,
    CAST(NULL AS FLOAT64) AS long,
    bundle_product_id AS bundleItemId,
    CAST(NULL AS FLOAT64) AS flashValuePaid
  FROM
    base_items_with_bag d
    INNER JOIN `flash-coffee-prod.iseller.iseller_orders` o ON o.order_id = d.order_id
    left join order_promo op on op.order_id = o.order_id
    LEFT JOIN (
      SELECT sic, SIC_Category_1
      FROM `analytics.apac_product_master`
      WHERE country = 'ID'
    ) pc ON LEFT(d.sku, 7) = pc.sic
)

--   select order_id, product_id, product_name,product_generic_name,mod_product_name, sku, quantity,
--   subtotal, total_order_amount,
--   tax_amount,0 tax_amount_include,
--   additional_charge,discount_amount,discount_order_amount,
--   disc_promo_amount_total,
--   disc_promo_amount_total - (disc_promo_amount_total / (1 + tax_percentage/100)) tax_disc_promo,
--   disc_promo_amount_1,disc_promo_amount_2,disc_promo_amount_3,tax_percentage,base_price,
--   i_promotions,i_discounts,
--   mod_modifier_product_sku, bundle_product_id
--   from base_items
-- where order_id = 'e19891bb-8dc4-44fa-b1e1-0cadff33aef1'

-- select order_id,i_promotions,i_discounts from base_items
-- where i_promotions <> '[]' or i_discounts <> '[]'

SELECT
-- items_subtotal,
  *
FROM
  IsellerFact
  -- where true
  -- and refid = 'e19891bb-8dc4-44fa-b1e1-0cadff33aef1'
  -- and refid = 'a9eee2c7-7d02-4512-b280-e5b97da01706'
  -- and localdate >= '2026-01-20'
ORDER BY
  localdate DESC, localtime desc, refId ;