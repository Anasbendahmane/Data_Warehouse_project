--Fact_sales

--- Data Lookup Transformation to replace original business keys with Surrogate Keys from your Gold dimensions, 
---ensuring your Fact table is optimized for performance and decoupled from source system changes.
CREATE VIEW gold.fact_sales as(
select
	   sls_ord_num as order_number
      ,prd.product_key
      ,cust.customer_key
      ,sls_order_dt as order_date
      ,sls_ship_dt as ship_date
      ,sls_due_dt as due_date
      ,sls_sales as Sales
      ,sls_quantity as quantity
      ,sls_price as price
     
from silver.crm_sales_details as sls 
LEFT JOIN gold.dim_customers as cust on sls.sls_cust_id = cust.customer_id
LEFT JOIN gold.dim_products as prd on sls.sls_prd_key =prd.product_number
)