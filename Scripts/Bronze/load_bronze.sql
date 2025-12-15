CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	BEGIN TRY
		PRINT 'Loading Data into the bronze layer';
		PRINT '--------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------';
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\MSI\Dropbox\Mon PC (DESKTOP-LC36MF4)\Desktop\Data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- Instructs the loader to skip the first row (the header) of the CSV file
			FIELDTERMINATOR =',', -- how the data is splited
			TABLOCK ---- Acquires an exclusive lock on the entire table for the duration of the bulk load operation to ensure data integrity and faster loading
		);


		-------------------------------------------------
		TRUNCATE TABLE bronze.crm_prd_info


		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\MSI\Dropbox\Mon PC (DESKTOP-LC36MF4)\Desktop\Data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		------------------------------------------------------------
		TRUNCATE TABLE bronze.crm_sales_details


		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\MSI\Dropbox\Mon PC (DESKTOP-LC36MF4)\Desktop\Data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '--------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------';
		TRUNCATE TABLE bronze.erp_cust_az12


		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\MSI\Dropbox\Mon PC (DESKTOP-LC36MF4)\Desktop\Data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--------------------------------------------------------

		TRUNCATE TABLE bronze.erp_loc_a101

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\MSI\Dropbox\Mon PC (DESKTOP-LC36MF4)\Desktop\Data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		------------------------------------------------------------------
		TRUNCATE TABLE bronze.erp_px_cat_g1v2


		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\MSI\Dropbox\Mon PC (DESKTOP-LC36MF4)\Desktop\Data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	END TRY
	BEGIN CATCH
		PRINT '-------------------------------';
		PRINT 'An error occured';
		PRINT 'ERROR MESSAGE: '+ ERROR_MESSAGE();
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
		PRINT 'ERROR LINE: ' + CAST(ERROR_LINE() as NVARCHAR(50));
		PRINT 'ERROR PROCEDURE: ' + ERROR_PROCEDURE();
	END CATCH
END
