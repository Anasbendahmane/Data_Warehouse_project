-----Data quality checks after joining the tables for products
--1/ check for  duplicates

select
	t.prd_key
from(

	select
		pdi.prd_id,
		pdi.ctg_id,
		pdi.prd_key,
		pdi.prd_nm,
		pdi.prd_cost,
		pdi.prd_line,
		cat.cat,
		cat.subcat,
		cat.maintenance,
		pdi.prd_start_dt,
		pdi.prd_end_dt
	from silver.crm_prd_info as pdi
	LEFT JOIN silver.erp_px_cat_g1v2 as cat on pdi.ctg_id =cat.id
	where prd_end_dt is null -- filter out all historical records and keep only the current records
)as t
group by t.prd_key
having COUNT(*) > 1
