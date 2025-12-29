---Customers View
CREATE VIEW gold.dim_customers AS(
select 
	ROW_NUMBER() over(order by cci.cst_create_date)  as customer_key, -- generate a surrogote key to connect the data model
	cci.cst_id as customer_id, 
	cci.cst_key as customer_number,
	cci.cst_firstname as first_name,
	cci.cst_lastname as last_name,
	cci.cst_marital_status as marital_status,
	CASE
		WHEN cci.cst_gndr = eca.gen THEN cci.cst_gndr	
		WHEN cci.cst_gndr != eca.gen and cci.cst_gndr != 'Unkown' THEN cci.cst_gndr	
		WHEN cci.cst_gndr = 'Unkown' and eca.gen IS NULL THEN cci.cst_gndr
		WHEN cci.cst_gndr = 'Unkown' and eca.gen IS NOT NULL THEN eca.gen
	END as gender,
	eca.bdate as birthday,
	eloc.cntry as country,
	cci.cst_create_date as create_date

from silver.crm_cust_info as cci
LEFT JOIN silver.erp_cust_az12 as eca
ON eca.cid = cci.cst_key
LEFT JOIN silver.erp_loc_a101 as eloc
ON eloc.cid = eca.cid
)
