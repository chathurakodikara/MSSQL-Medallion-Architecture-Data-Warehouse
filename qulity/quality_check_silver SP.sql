/*
===============================================================================
Stored Procedure: Data Quality Check in Silver Layer (Test -> Silver)
===============================================================================
Author: Chathura Kodikara               Initiate Date: 2026-04-19

Script Purpose:
    This stored procedure check data quality in the 'silver' schema. 
    It performs the following actions:
    - check duplicate.
    - string values has extra spaces.

Parameters:
    None. 
	  This stored procedure does not accept any parameters.

Usage Example:
    EXEC silver.quality_check;

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
CREATE OR ALTER PROCEDURE silver.quality_check AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME, @qty INT; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '================================================';
		PRINT 'Quality Check in Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Quality Check CRM Tables: crm_cust_info';
		PRINT '------------------------------------------------';

		--Check Null or Duplicate
		SET @start_time = GETDATE();
		PRINT '>> Check Null or Duplicate';



		SELECT @qty = COUNT(cst_id)
		FROM   silver.crm_cust_info
		GROUP  BY cst_id
		HAVING cst_id IS NULL
				OR COUNT(cst_id) > 1;

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';


		--Check string has spaces

		SET @start_time = GETDATE();
		PRINT '>> Check cst_firstname and cst_lastname has spaces';

		SELECT @qty = COUNT(*)
		FROM   silver.crm_cust_info
		WHERE  cst_firstname <> TRIM(cst_firstname)
				OR cst_lastname <> TRIM(cst_lastname);

        SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';

		----------------------------------
		PRINT '------------------------------------------------';
		PRINT 'Quality Check CRM Tables: crm_prd_info';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Check prd_id Null or Duplicate';

		SELECT @qty = COUNT(prd_id)
		FROM .[silver].[crm_prd_info]
		GROUP BY prd_id
		HAVING COUNT(prd_id) > 1 OR prd_id IS NULL

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';


		-- prd_cost has '-' values or null value
		SET @start_time = GETDATE();
		PRINT '>> Check prd_cost coll has "-" values or null value';

		SELECT @qty = COUNT(prd_id)
		FROM [silver].[crm_prd_info]
		WHERE prd_cost < 0 OR prd_cost IS NULL

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';


		-- prd_key and prd_nm has spaces

		SET @start_time = GETDATE();
		PRINT '>> Check prd_key and prd_nm has spaces';

		SELECT @qty = COUNT(prd_id)
		FROM [DataWarehouse].[silver].[crm_prd_info]
		WHERE prd_key <> TRIM(prd_key) OR prd_nm <> TRIM(prd_nm)

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';

		-- prd_start_dt has null value

		SET @start_time = GETDATE();
		PRINT '>> Check prd_start_dt has null value';

		SELECT @qty = COUNT(prd_id)
		FROM [DataWarehouse].[silver].[crm_prd_info]
		WHERE prd_start_dt IS NULL

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';

		--Check prd_start_dt > prd_end_dt
		SET @start_time = GETDATE();
		PRINT '>> Check prd_start_dt > prd_end_dt';

		SELECT @qty = COUNT(prd_id)
		FROM [DataWarehouse].[silver].[crm_prd_info]
		WHERE prd_start_dt > prd_end_dt 


		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';


		----------------------
		PRINT '------------------------------------------------';
		PRINT 'Quality Check CRM Tables: crm_sales_details';
		PRINT '------------------------------------------------';

		-- Check cst_id values exisit in crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Check cst_id values exisit in crm_cust_info';

		SELECT @qty = COUNT(sls_ord_num) 
		FROM silver.crm_sales_details
		WHERE sls_cust_id NOT IN (select cst_id FROM silver.crm_cust_info)

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';

		
		-- Check sls_prd_key values exisit in crm_prd_info

		SET @start_time = GETDATE();
		PRINT '>> Check sls_prd_key values exisit in crm_prd_info';

		SELECT @qty = COUNT(sls_ord_num) 
		FROM silver.crm_sales_details
		WHERE sls_prd_key NOT IN (select prd_key FROM silver.crm_prd_info)

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';

		-- Check Order Date always lower than Order Date and Due Date
		SET @start_time = GETDATE();
		PRINT '>> Check Order Date always lower than Order Date and Due Date';

		SELECT @qty = COUNT(sls_ord_num) 
		FROM silver.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';

		-- Check Sales, Quantity and Price has incorect values
		SET @start_time = GETDATE();
		PRINT '>> Check Sales, Quantity and Price has incorect values';

		SELECT @qty = COUNT(*)
		FROM silver.crm_sales_details
		WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';

		--------------------------------


		PRINT '------------------------------------------------';
		PRINT 'Quality Check ERP Tables: erp_cust_az12';
		PRINT '------------------------------------------------';

		 -- Check duplicate customers
		SET @start_time = GETDATE();
		PRINT '>> Check duplicate customers';
		
		SELECT @qty =  COUNT(cid)
		  FROM silver.erp_cust_az12
		  GROUP BY cid
		  HAVING COUNT(cid) > 1


		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';


  		-- Check Birthday is larger than currant day
		SET @start_time = GETDATE();
		PRINT '>> Check cst_id values exisit in crm_cust_info';
		
		SELECT @qty = COUNT(cid)
		  FROM silver.erp_cust_az12
		  WHERE bdate > CAST(GETDATE() AS DATE)


		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';



		-- Age is gratert than 100 years
		SET @start_time = GETDATE();
		PRINT '>> Check Age is gratert than 100 years';
		
		SELECT @qty = COUNT(cid)
		  FROM silver.erp_cust_az12
		  WHERE DATEDIFF(YEAR, bdate, GETDATE()) > 100


		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';



 

		-- Birthday is larger than currant day
		SET @start_time = GETDATE();
		PRINT '>> Check Birthday is larger than currant day';
		
		SELECT @qty = COUNT(cid)
		  FROM silver.erp_cust_az12
		  WHERE bdate > CAST(GETDATE() AS DATE)


		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';


		-- Check Gender
		SET @start_time = GETDATE();
		PRINT '>> Check Gender is incorrect';
		
		SELECT @qty =  COUNT(cid)
		  FROM silver.erp_cust_az12
		  WHERE gen NOT IN ('Female', 'Male', 'n/a')


		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';


		-- Check customers in CRM CUSTOMER table
		SET @start_time = GETDATE();
		PRINT '>> Check customers are not in CRM CUSTOMER table';
		
		SELECT @qty =  COUNT(cid)
		FROM silver.erp_cust_az12
		WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)



		SET @end_time = GETDATE();
		PRINT '>> Result: Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> Found: ' + CAST(ISNULL(@qty, 0) AS NVARCHAR) + ' records';
		PRINT '>> -------------';



		--EXEC silver.quality_check;


END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING QUALITY CHECK SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
