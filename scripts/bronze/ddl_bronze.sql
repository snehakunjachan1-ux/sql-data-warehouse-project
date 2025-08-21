/* 
=======================================================================================
DDL SCRIPTS : CREATE BRONZE TABLES
=======================================================================================
SCRIPTS PURPOSE :
            THIS SCRIPTS CREATES TABLES IN THE BRONZE SCHEMA , DROPPING EXISTING TABLES
IF THEY ALREADY EXIST.
RUN THIS SCRIPTS TO RE-DEFINE THE DDL STRUCTURE OF BRONZE TABLES 
========================================================================================
*/
-- create bronze tables and create data warehouse name DataWarehouse --

USE DataWarehouse;

select * from bronze.crm_cust_info;

select count(*) from bronze.crm_cust_info;

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
	
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY 

			print '=====================================================';
			print  'Loading Bronze Layer';
			print '---------------------------------------------------------------';
			
			print '===================================================';
			print 'Loading CRM Tables';
			print '============================================================';
			
			SET @start_time = GETDATE();

			print'>> inserting data into : bronze.crm_prd_info'
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\sneha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @end_time = GETDATE();
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\sneha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);

			print '===================================================';
			print 'Loading ERP Tables';
			print '===========================================';
			
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\sneha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\sneha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\sneha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);

	END TRY
	BEGIN CATCH 
	    PRINT'=================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT'=================================='
	END CATCH
END

select * from bronze.erp_px_cat_g1v2;
select * from bronze.erp_cust_az12;
select * from  bronze.erp_loc_a101;

select count(*) from bronze.erp_px_cat_g1v2;
select count(*)  from bronze.erp_cust_az12;
select count(*) from  bronze.erp_loc_a101;

