-- Data quality checks for Sales_details
--1/ Check for unwanted spaces
Select
	*
from bronze.crm_sales_details
where sls_prd_key != TRIM(sls_prd_key)

Select
	*
from bronze.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num)

-- Result: No unwanted spaces detected

---2 /Check for Referential Integrity

select
	*
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from bronze.crm_cust_info)

select
	*
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

-- Result: 0 orphaned records detected 


---3/ check for invalid dates 
---We need to ensure date integers are positive and non-zero
---before attempting to cast them to a DATE format
select
	sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 

select
	sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 


select
	sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 

---- Result: No negative or zero dates were found in "sls_due_dt" and "sls_ship_dt" columns.
---  However, the "sls_order_dt" column contains 17 records with a value of 0 so we need to replace it with null


--- 4/ Check data consistency : Between sales ,quantity and price (must not be NULL, ZERO or negative)
--- Sales = price * quantity 

select
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR
	  sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 OR 
	  sls_price * sls_quantity != sls_sales 

--Result: 35 records detected with consistency issues 
--(Mathematical Inconsistencies/Missing Values (NULLs)/Invalid Numeric Values)
