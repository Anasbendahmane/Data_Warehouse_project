-----Data quality checks after joining the tables for customers

--1/ check for  duplicates
select
	t.cst_key,
	COUNT(*) as total_duplicates
from(
	select 
		cci.cst_id,
		cci.cst_key,
		cci.cst_firstname,
		cci.cst_lastname,
		cci.cst_marital_status,
		cci.cst_gndr,
		cci.cst_create_date,
		eca.bdate,
		eca.gen,
		eloc.cntry

	from silver.crm_cust_info as cci
	LEFT JOIN silver.erp_cust_az12 as eca
	ON eca.cid = cci.cst_key
	LEFT JOIN silver.erp_loc_a101 as eloc
	ON eloc.cid = eca.cid
) as t
group by t.cst_key
having COUNT(*) > 1

--- Result: 0 duplicate records detected.



 --- 2/ Data Integration: Gender Source Conflict Analysis
select 
		
		cci.cst_gndr,
		eca.gen
from silver.crm_cust_info as cci
LEFT JOIN silver.erp_cust_az12 as eca
ON eca.cid = cci.cst_key
LEFT JOIN silver.erp_loc_a101 as eloc
ON eloc.cid = eca.cid	
group by cci.cst_gndr,eca.gen

--Result: We found critical gender conflicts where CRM and ERP sources directly disagree (Male vs. Female) 
--- or contain inconsistent placeholders like "unknown," and NULLs.