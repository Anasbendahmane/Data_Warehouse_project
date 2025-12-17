---- First issue
--- Check fon unwanted spaces

Select 
	cst_firstname
from bronze.crm_cust_info
where cst_firstname != Trim(cst_firstname)

select
	cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

select
	cst_gndr
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr)

select
	cst_marital_status
from bronze.crm_cust_info
where cst_marital_status != TRIM(cst_marital_status)

--- The 'cst_firstname' and 'cst_lastname' columns contain records with unwanted leading or trailing spaces.

---- Second issue
---- Check For Nulls and Duplicates in Primary Key

SELECT
	cst_id,
	COUNT(cst_id) as NB_of_duplicates
from bronze.crm_cust_info
group by cst_id
having COUNT(cst_id) > 1 

SELECT
	*
from bronze.crm_cust_info
where cst_id is NULL

----- the issue here that we have multiple version records for the same cst_id, each with a different cst_create_date
