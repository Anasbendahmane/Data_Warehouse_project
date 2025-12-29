--products view

CREATE VIEW gold.dim_products AS (
	select
		ROW_NUMBER() over (order by pdi.prd_id) as product_key,
		pdi.prd_id as product_id,
		pdi.ctg_id as category_id,
		pdi.prd_key as product_number,
		pdi.prd_nm as product_name,
		pdi.prd_cost as product_cost,
		pdi.prd_line as product_line,
		cat.cat as category,
		cat.subcat as subcategory,
		cat.maintenance,
		pdi.prd_start_dt
	from silver.crm_prd_info as pdi
	LEFT JOIN silver.erp_px_cat_g1v2 as cat on pdi.ctg_id =cat.id
	where prd_end_dt is null -- filter out all historical records and keep only the current records
)
