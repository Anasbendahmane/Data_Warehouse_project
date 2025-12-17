--- fixing the first issue

select
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
from bronze.crm_cust_info;


--- Fixing the second issue
/*
select
	*
from bronze.crm_cust_info
where cst_id = 29466;

select
	*
from bronze.crm_cust_info
where cst_id = 29483;

select
*
from bronze.crm_cust_info
where cst_id = 29449;
*/

---- The challenge here is that we have multiple version records for the same cst_id, each with a different cst_create_date
---- Use a ranking function to isolate and keep only the most recent version of each customer record

WITH cte_ranking_records AS (
	select
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date,
		ROW_NUMBER() over(Partition by cst_id order by cst_create_date desc) ranking_records

	from bronze.crm_cust_info)


select
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
from cte_ranking_records
where ranking_records = 1 
