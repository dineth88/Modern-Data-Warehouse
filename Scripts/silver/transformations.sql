-- creatings stored procedures
EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		--IMPORTING DATA SOURCES(csv)
		SET @batch_start_time = GETDATE();
		print('============================LOADING BRONZE LAYER==============================');

		PRINT('======================LOADING CRM TABLES=====================')

		-- ================== crm_cust_info ===========================
		SET @start_time = GETDATE();
		PRINT('>>Truncating table: silver.crm_cust_info');

		TRUNCATE TABLE silver.crm_cust_info
		PRINT('>> Inserting data into silver crm_cust_info')

		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		select
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		-- Map marital_status field into S --> Single and M --> Married
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a' -- Adding default for null and other values
		END cst_marital_status,
		-- Map gender field into M --> Male and F --> Female
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a' -- Adding default for null and other values
		END cst_gnder,
		cst_create_date
		from (select 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info)t where flag_last = 1

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds');
		PRINT('**********************************************************************')

		--  ================== crm_prd_info ===========================
		SET @start_time = GETDATE();
		PRINT('>>Truncating table: silver.crm_prd_info');

		TRUNCATE TABLE silver.crm_prd_info
		PRINT('>> Inserting data into silver crm_prd_info')

		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
			prd_id,
			-- Fist 5 characters --> category id (CO-RF --> CO_RF), remaining product_id
			-- SUBSTRING(field, start from, length)
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS car_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			-- Converting null --> 0 for further calculations
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST (prd_start_dt AS DATE) AS prd_start_dt,
			-- start date < end date
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
		from bronze.crm_prd_info
		WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
		SELECT sls_prd_key FROM bronze.crm_sales_details)

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds');
		PRINT('**********************************************************************')

		--  ================== crm_sales_details ===========================
		SET @start_time = GETDATE();
		PRINT('>>Truncating table: silver.crm_sales_details');

		TRUNCATE TABLE silver.crm_sales_details
		PRINT('>> Inserting data into silver crm_sales_details')

		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select 
		sls_ord_num,
		RIGHT(sls_prd_key, 2) AS sls_prd_key,
		sls_cust_id,
		-- date restrictions
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		-- Check data consistency between Sales, Quantity and Price
		-- >> Sales = Qunatity * Price
		-- >> Values must be NOT NULL, zer or negative
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price -- Derive price if original value is invalid
		FROM bronze.crm_sales_details

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds');
		PRINT('**********************************************************************')

		--  ================== erp_cust_az12 ===========================
		SET @start_time = GETDATE();
		PRINT('>>Truncating table: silver.erp_cust_az12');

		TRUNCATE TABLE silver.erp_cust_az12
		PRINT('>> Inserting data into silver erp_cust_az12')

		-- prefix removed cid column will use as a foreign key
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END cid,
		bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds');
		PRINT('**********************************************************************')

		-- ========================== erp_loc_a101 =======================
		SET @start_time = GETDATE();
		PRINT('>>Truncating table: silver.erp_loc_a101');

		TRUNCATE TABLE silver.erp_loc_a101
		PRINT('>> Inserting data into silver erp_loc_a101')

		INSERT INTO silver.erp_loc_a101
		(cid, cntry)
		SELECT 
		REPLACE(cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END as cntry --Normalize and handling missing or blank country codes
		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds');
		PRINT('**********************************************************************')


		--======================= erp_px_cat_g1v2 ===========================
		SET @start_time = GETDATE();
		PRINT('>>Truncating table: silver.erp_px_cat_g1v2');

		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT('>> Inserting data into silver erp_px_cat_g1v2')

		INSERT INTO silver.erp_px_cat_g1v2 
		(id, cat,subcat,maintenance )
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds');
		PRINT('**********************************************************************')
		SET @batch_end_time = GETDATE();
		PRINT('===============Loading Bronze silver is completed=================');
		PRINT('Total Load duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds');
		PRINT('**********************************************************************')

	END TRY
	BEGIN CATCH 
		PRINT('==============================ERROR OCCURED DURING LOADING SILVER LAYER====================');
		PRINT('error message' + ERROR_MESSAGE());
		PRINT('error message' + CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('error message' + CAST(ERROR_STATE() AS NVARCHAR));
	END CATCH
END