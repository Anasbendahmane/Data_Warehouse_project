CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
	DECLARE @start DATETIME ,@end DATETIME , @startsilver DATETIME ,@endsilver DATETIME
	SET @startsilver = GETDATE()
	BEGIN TRY

	-------------------------------------CRM Tables----------------------------------------
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


	SET @start = GETDATE()
	PRINT 'TRUNCATING TABLE silver.crm_cust_info ';
	TRUNCATE TABLE silver.crm_cust_info;

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

	SET @end = GETDATE()
	PRINT 'the time to load the data into the customer table is : ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR);
	------------------------Product info-----------------------
	--The prd_key contains many informations (split the string into informations and deriving two new columns)

	SET @start = GETDATE()

	PRINT 'Truncating Table : Silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

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


	SET @end = GETDATE()
	PRINT 'the time to load the data into the product table is : ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR);

	------------------------Sales details-----------------------


	SET @start = GETDATE()
	PRINT 'Truncating Table : silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;

	INSERT INTO silver.crm_sales_details(

		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 

	)



	SELECT 
		 sls_ord_num,
		 sls_prd_key,
		 sls_cust_id,
		 CASE 
			  WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 then NULL -- Handle invalid dates (0, negative,or incorrect length) 
			  ELSE CAST(CAST(sls_order_dt as VARCHAR) AS DATE)
		 END as sls_order_dt ,
		 CAST(CAST(sls_ship_dt as varchar) AS DATE) as sls_ship_dt,
		 CAST(CAST(sls_due_dt as varchar) AS date) as sls_due_dt,
		 CASE 
			WHEN sls_sales <= 0 OR sls_sales IS NULL OR abs(sls_sales) != sls_quantity * ABS(sls_price) THEN ABS(sls_price) * ABS(sls_quantity) --if sales is null or negative then quantity * price
			ELSE sls_sales
		 END as sls_sales,
		 ABS(sls_quantity) as quantity ,
		 CASE 
			WHEN sls_price IS NULL or sls_price = 0 THEN ABS(sls_sales)/ABS(sls_quantity) 
			else sls_price
		 END AS sls_price
	from bronze.crm_sales_details

	SET @end = GETDATE()
	PRINT 'the time to load the data into the sales table is : ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR);

	--------------------------------------------ERP TABLES---------------------------------------

	------------------------------- Customer infos----------------------------------


	SET @start = GETDATE()
	
	PRINT 'Truncating Table : silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)

	select
		CASE 
			WHEN LEN(cid) != 10 THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE 
			 WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE 
			WHEN gen ='M' then 'Male'
			WHEN gen ='F' then 'Female'
			WHEN gen IS NULL OR gen =' ' then 'unkown'
			ELSE gen
		END AS gen
	from bronze.erp_cust_az12

	SET @end = GETDATE()
	PRINT 'the time to load the data into the customer table is : ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR);

	------------------------------- Customer Location----------------------------

	SET @start = GETDATE()

	PRINT 'Truncating Table : silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;


	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)

	select	
		REPLACE(cid,'-','') as cid, -- we need to repalce the '-' to have the same cid in order to join the crm_cust_info
		CASE
			WHEN UPPER(TRIM(cntry)) ='US' OR UPPER(TRIM(cntry)) ='USA' THEN 'United States'
			WHEN UPPER(TRIM(cntry)) ='DE' THEN 'Germany'
			WHEN cntry =' ' OR cntry IS NULL THEN 'Unkown'
			ELSE cntry
		END AS cntry

	from bronze.erp_loc_a101
	SET @end = GETDATE()
	PRINT 'the time to load the data into the customer location is : ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR);

	------------------------------- Product Categories ----------------------------


	SET @start = GETDATE()
	PRINT 'Truncating Table : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;


	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)



	select
		id,
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2

	SET @end = GETDATE()
	PRINT 'the time to load the data into the product categories table is : ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR);
	END TRY

	BEGIN CATCH
		PRINT '-------------------------------';
		PRINT 'An error occured';
		PRINT 'ERROR MESSAGE: '+ ERROR_MESSAGE();
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
		PRINT 'ERROR LINE: ' + CAST(ERROR_LINE() as NVARCHAR(50));
		PRINT 'ERROR PROCEDURE: ' + ERROR_PROCEDURE();
	END CATCH

	SET @endsilver = GETDATE()
	PRINT 'the total time to load the data into the silver layer take : ' + CAST(DATEDIFF(second,@startsilver,@endsilver) AS NVARCHAR);
END
