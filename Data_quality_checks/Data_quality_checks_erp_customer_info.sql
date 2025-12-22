-- Data quality checks for Sales_details
--1/ Check for unwanted spaces

select
	*
from bronze.erp_cust_az12
where gen != TRIM(gen)

-- Result: No unwanted spaces detected

---2/ Check for duplicates or null values in the primary key

select
	COUNT(*) as nb_of_duplicates
from bronze.erp_cust_az12
group by cid
having COUNT(*) > 1

select
	*
from bronze.erp_cust_az12
where cid is null

----- Result: No duplicates detected (The query returns 0 records)

---3/ check Data consistency

select distinct
	gen
from bronze.erp_cust_az12


-- There is 6 diffrent values  (M/F/Male/Female/NULL/' ')
-- so we want to store clear values rather than using abbreviated terms

---4/ Check for invalid dates

select
	*
from bronze.erp_cust_az12
where bdate is null or LEN(bdate) != 10 

--Result: 0 records detected with date issues.

---5/ Check the range of the date
select
	*
from bronze.erp_cust_az12
where bdate < '1919-01-01' or bdate > GETDATE()

---Result: Multiple records detected with out-of-range dates (Future Dates / Historical Dates)

