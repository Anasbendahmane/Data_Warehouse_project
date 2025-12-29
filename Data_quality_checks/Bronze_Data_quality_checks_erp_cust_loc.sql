-- Data quality checks for ERP customer location
--- 1/ Check data consistency
select distinct
	cntry
from bronze.erp_loc_a101

---Result: Multiple standardization issues detected in country names( Inconsistent Naming / Missing Data)

--- 2/ Change the cid to join the crm customer info 
select
	cid
from bronze.erp_loc_a101

select 
	cst_key
from silver.crm_cust_info

--- Resuult : we need to replace the '-' with '' to join the tables