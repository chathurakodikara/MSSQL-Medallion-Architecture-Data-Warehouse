/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Author: Chathura Kodikara               Initiate Date: 2026-04-19

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
********
Warnings: 

===============================================================================
Updates
-------------------------------------------------------------------------------
Date        |Author              |Ticket      |Description
-------------------------------------------------------------------------------
2026-04-19  |Chathura Kodikara   |Start       |Initiate


===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @rows_inserted INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- 1. Loading silver.crm_cust_info
        SET @start_time = GETDATE();
        
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        WITH cte_CustomerInfo AS (
            SELECT 
                cst_id,
                cst_key,
                TRIM(cst_firstname) AS cst_firstname,
                TRIM(cst_lastname) AS cst_lastname,
                CASE UPPER(TRIM(cst_marital_status))
                    WHEN 'S' THEN 'Single'
                    WHEN 'M' THEN 'Married'
                    ELSE 'n/a'
                END AS cst_marital_status,
                CASE UPPER(TRIM(cst_gndr)) -- Added Upper/Trim for consistency
                    WHEN 'M' THEN 'Male'
                    WHEN 'F' THEN 'Female' -- Corrected typo
                    ELSE 'n/a'
                END AS cst_gndr,
                cst_create_date,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as row_num
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        )
        INSERT INTO silver.crm_cust_info (
            cst_id, 
            cst_key, 
            cst_firstname, 
            cst_lastname, 
            cst_marital_status, 
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        FROM cte_CustomerInfo
        WHERE row_num = 1;

        -- Capture metadata
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Inserted: ' + CAST(@rows_inserted AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        --------------------------------------------------------------------
           -- 2. Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        
        PRINT '>> Inserting Data Into: silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info (
            prd_id
            ,cat_id
            ,prd_key
            ,prd_nm 
            ,prd_cost 
            ,prd_line
            ,prd_start_dt
            ,prd_end_dt
        )
        SELECT prd_id
              ,REPLACE(SUBSTRING(prd_key,1,5), '-','_') cat_id
              ,SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key
              ,prd_nm
              ,ISNULL(prd_cost,0) AS prd_cost
              ,CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountan'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Sport'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a'
                END prd_line
              ,CAST(prd_start_dt AS DATE) prd_start_dt
              ,CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) prd_end_dt
          FROM bronze.crm_prd_info

            -- Capture metadata
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Inserted: ' + CAST(@rows_inserted AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

         --------------------------------------------------------------------

          --------------------------------------------------------------------
           -- 3. Loading silver.crm_sales_details
        SET @start_time = GETDATE();
        
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        
        PRINT '>> Inserting Data Into: silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details (
            sls_ord_num
            ,sls_prd_key
            ,sls_cust_id
            ,sls_order_dt
            ,sls_ship_dt
            ,sls_due_dt
            ,sls_sales
            ,sls_quantity
            ,sls_price
        )

         SELECT sls_ord_num
            ,sls_prd_key
            ,sls_cust_id
            ,CASE 
                WHEN sls_order_dt < 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END sls_order_dt
            ,CASE 
                WHEN sls_ship_dt < 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END sls_ship_dt
            ,CASE 
                WHEN sls_due_dt < 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END sls_due_dt
            ,CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0  THEN sls_price/ISNULL(sls_quantity,1)
                WHEN sls_sales IS NULL AND sls_quantity IS NULL THEN 0
                ELSE sls_sales
            END sls_sales
            ,ISNULL(sls_quantity,1) sls_quantity
            ,CASE
                WHEN sls_price IS NULL OR sls_price <= 0  THEN sls_sales*ISNULL(sls_quantity,1)
                WHEN sls_sales IS NULL AND sls_quantity IS NULL THEN 0
                ELSE sls_price
                END sls_price
        FROM bronze.crm_sales_details

            -- Capture metadata
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Inserted: ' + CAST(@rows_inserted AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

           -- 4. Loading silver.erp_cust_az12
        SET @start_time = GETDATE();
        
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        
        PRINT '>> Inserting Data Into: silver.erp_cust_az12';

        INSERT INTO silver.erp_cust_az12 (
            cid
            ,bdate
            ,gen
        )
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,3,LEN(cid))
                ELSE cid
                END cid
             ,bdate
             ,CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
                END gen
          FROM bronze.erp_cust_az12

              -- Capture metadata
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Inserted: ' + CAST(@rows_inserted AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        ----------------------------

         -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



        -- Final Batch Summary
        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '=========================================='
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================='
    END CATCH
END;