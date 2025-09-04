/*
    ------------------------------------------------------
    Stored Procedure : Load Bronze Layer(Source -> Bronze)
    ------------------------------------------------------

    Scripts:
              This stored procedure loads data into 'bronze' schema from CSV files,
              It performs the following actions:
                   - Truncate the bronze tables
                   - Use the 'Bulk Insert' command to load the data
   --------------------------------------------------------------------------------------         
              Parameters:
                          None

              Example : EXEC bronze.load_bronze

   */


EXEC bronze.load_bronze


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
  BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME;
  SET @start_time = GETDATE();
  BEGIN TRY
PRINT('CRM Source Files Loading......')
TRUNCATE TABLE bronze.crm_cust_info
SET @start_time = GETDATE();
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\ankit\OneDrive\Dacument\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);

SET @end_time = GETDATE()

Print'>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'


TRUNCATE TABLE bronze.crm_prd_inf
SET @start_time = GETDATE();
BULK INSERT bronze.crm_prd_inf
FROM 'C:\Users\ankit\OneDrive\Dacument\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
   );
SET @end_time = GETDATE()
Print'>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'



TRUNCATE TABLE bronze.crm_sales_details
SET @start_time = GETDATE();
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\ankit\OneDrive\Dacument\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
   FIRSTROW = 2,
   FIELDTERMINATOR = ',',
   TABLOCK
   );
SET @end_time = GETDATE()
Print'>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

   
PRINT(' ')
PRINT('----------------------------------------------------------------------------------')
PRINT('                                                        ')
 PRINT('ERP Soruce files loading.....')
TRUNCATE TABLE  bronze.erp_cust_AZ12
SET @start_time = GETDATE();
BULK INSERT bronze.erp_cust_AZ12
FROM 'C:\Users\ankit\OneDrive\Dacument\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
    );


SET @end_time = GETDATE()
Print'>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

    

TRUNCATE TABLE bronze.erp_loc_A101
SET @start_time = GETDATE();
BULK INSERT bronze.erp_loc_A101
FROM 'C:\Users\ankit\OneDrive\Dacument\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
   FIRSTROW = 2,
   FIELDTERMINATOR = ',',
   TABLOCK
   );
 SET @end_time = GETDATE()
Print'>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
  

TRUNCATE TABLE bronze.erp_px_cat_g1v2
SET @start_time = GETDATE();
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\ankit\OneDrive\Dacument\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
 SET @end_time = GETDATE()
Print'>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

END TRY
BEGIN CATCH
            PRINT('Error occured during files loading')
            PRINT 'Error Message' + ERROR_MESSAGE()
            PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR)
            PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR)

END CATCH
print'-------------------------------------------------------------'
SET @end_time = GETDATE()

Print'>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'

  END
