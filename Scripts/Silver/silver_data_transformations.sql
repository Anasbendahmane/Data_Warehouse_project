USE Datawarehousing;
------------ customer infos-----------------
--- fixing the first issue
/*
select
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
from bronze.crm_cust_info;
*/

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


INSERT INTO silver.crm_cust_info(
	cst_id ,
	cst_key ,
	cst_firstname ,
	cst_lastname ,
	cst_marital_status ,
	cst_gndr ,
	cst_create_date
)

select
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single' -- we use "upper" just in case lowercase values appear
		WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married '
		ELSE 'Unkown'
	END AS cst_maritial_status,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' -- we use "upper" just in case lowercase values appear
		WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
		ELSE 'Unkown'
	END AS cst_gndr,
	cst_create_date
from cte_ranking_records
where ranking_records = 1 

------------------------Product info-----------------------
--The prd_key contains many informations (split the string into informations and deriving two new columns)

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
BEGIN
    DROP TABLE silver.crm_prd_info;
END
GO

CREATE TABLE silver.crm_prd_info(

	prd_id int,
	ctg_id nvarchar(50) ,
    prd_key nvarchar(50), 
	prd_nm nvarchar(50),
	prd_cost int ,
	prd_line nvarchar(50),
	prd_start_dt Datetime,
	prd_end_dt Datetime,
	dwh_create_date Datetime2 DEFAULT GETDATE()
	

)

INSERT INTO Silver.crm_prd_info(
	prd_id,
	ctg_id  , 
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)

SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as ctg_id  , ---we repalce '-' with '_' to join this table with erp_px_cat_g1v2 table
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key, -- we need this column to join with the table sales_details (sls_prd_key)
	prd_nm,
	COALESCE(prd_cost,0) as prd_cost,
	CASE
		WHEN UPPER(TRIM(prd_line)) ='M' then 'Mountain' -- mountain bike "VTT"
		WHEN UPPER(TRIM(prd_line)) = 'S' then 'Other Sales' -- Often used for accessories (helmets, clothing, components)
		WHEN UPPER(TRIM(prd_line)) ='T' then	'Touring' -- Touring bike " Vélo de Randonnée "
		WHEN UPPER(TRIM(prd_line)) = 'R' then 'Road' -- Road bike "Vélo de Route / Vélo de Course"
		ELSE 'Unkown'
	END as prd_line,
	prd_start_dt,
	DATEADD(day,-1,LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt
from bronze.crm_prd_info


