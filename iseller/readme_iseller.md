-- data for iseller_orders
select * except (order_details) from  `flash-coffee-prod.iseller.iseller_orders` 
where order_date >= '2026-03-01'

-- data for 
select od.* FROM `flash-coffee-prod.iseller.iseller_order_details`  od
  left join `flash-coffee-prod.iseller.iseller_orders`  o  on od.order_id = o.order_id
where order_date >= '2026-03-01'

-- data for draft_qc_basic_metrics_iseller
select * from flash-coffee-prod.iseller.draft_qc_basic_metrics_iseller
where LocalDate >= '2026-03-01'