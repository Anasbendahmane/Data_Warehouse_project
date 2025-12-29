-- Data quality checks for ERP product categories

--1/ Check for unwanted spaces

select
	*
from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat)

-- Result: No unwanted spaces detected

---2/ check Data consistency
select distinct
	cat
from bronze.erp_px_cat_g1v2

select distinct
	subcat
from bronze.erp_px_cat_g1v2

select distinct
	maintenance
from bronze.erp_px_cat_g1v2

--Result: All is OK without duplicates or spelling variations