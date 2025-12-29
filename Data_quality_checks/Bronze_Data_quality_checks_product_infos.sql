-- Data quality checks for products
---1/ Check For Nulls and Duplicates in Primary Key

select
	COUNT(*) as duplicates_rows,
	prd_id
from bronze.crm_prd_info
group by prd_id
having COUNT(*) > 1

select
	prd_id
from bronze.crm_prd_info
where prd_id is null

----- Result: No duplicates detected (The query returns 0 records)


---2/  The prd_key contains many informations (split the string into informations and deriving two new columns)
SELECT
	prd_key
from bronze.crm_prd_info

--3/ Check for unwanted spaces
SELECT
	prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- Result: No unwanted spaces detected

--4 / Check for NUlls or Negative numbers
SELECT
	prd_id,
	prd_cost
from bronze.crm_prd_info
Where prd_cost != ABS(prd_cost) or prd_cost IS NULL

-- There is two records with null cost so we need to replace NULLs with 0

--5/ -- check Data consistency
SELECT DISTINCT
	prd_line
from bronze.crm_prd_info

-- There is 5 diffrent values  (M/R/S/T/NULL)
-- so we want to store clear values rather than using abbreviated terms


---6/ Check for invalid Date ( END date must be after the start date)
SELECT
	*
from bronze.crm_prd_info
where prd_start_dt > prd_end_dt

---- Detected records where the start date is logically after the end date.
---- Solution: For each product, we will ignore the original end_date and 
---- replace it with the start date of the following record minus one day. 
---- This ensures a continuous timeline and prevents history overlaps.
