/*
---------------------------------------------------------------------
Stored Procedure : Load Silver Layer (bronze - silver)
---------------------------------------------------------------------
Script purpose :
                 This procedure performs the ETL (Extract Transform Load) process to populate
                 the 'silver' schema tables from the 'bronze' schema.

Action Perfomed :
                 - Trucncate silver layer
                 - Inserts transformed and cleaned data from bronze into silver tables

Parameters  :
               None

Examples :
           EXEC silver.load_silver
*/

EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver 
AS
BEGIN

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

SET @batch_start_time  = GETDATE()
BEGIN TRY 
PRINT('>> Truncating table : silver.crm_cust_info')
TRUNCATE TABLE silver.crm_cust_info
PRINT('>> Inserting data into : silver.crm_cust_info')

 SET @start_time = GETDATE()
INSERT INTO silver.crm_cust_info(
   cst_id,
   cst_key,
   cst_firstname,
   cst_lastname,
   cst_material_status,
   cst_gndr,
   cst_create_date
   )

SELECT cst_id,
       cst_key,
       TRIM(cst_firstname) as cst_firstname,
       TRIM(cst_lastname) as cst_lastname,
       CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
       END cst_material_status,
       CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a'
       END  cst_gndr,
       cst_create_date 
   from(
SELECT * ,
       ROW_NUMBER() OVER(Partition by cst_id order by cst_create_date desc) as flag_last
       from bronze.crm_cust_info
       where cst_id IS NOT NULL
       
       )t where flag_last = 1;

--SET end_time = GETDATE();

PRINT '>> Loading time :'+ CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)+' Seconds'
PRINT '--------------------------------------------------------------------'

PRINT('>> Truncating table : silver.crm_prd_inf')
TRUNCATE TABLE silver.crm_prd_inf
PRINT('>> Inserting data into : silver.crm_prd_inf')


 SET @start_time = GETDATE()
INSERT INTO silver.crm_prd_inf(
   prd_id,
   cat_id,
   prd_key,
   prd_nm,
   prd_cost,
   prd_line,
   prd_start_dt,
   prd_end_dt
   )


SELECT
       prd_id, 
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
	prd_nm ,
	ISNULL(prd_cost, 0) as prd_cost, 
	CASE UPPER(TRIM(prd_line))
	     WHEN 'M' THEN 'Mountain'
	     WHEN 'S' THEN 'Other Sales'
	     WHEN 'R' THEN 'Road'
	     WHEN 'T' THEN 'Touring'
	     ELSE 'n/a'
	END prd_line,
	CAST(prd_start_dt AS DATE) as prd_start_dt, 
	CAST(LEAD(prd_start_dt) OVER (partition by prd_key order by prd_start_dt) -1  AS DATE) as prd_end_dt
	FROM bronze.crm_prd_inf


    
   ---SET end_time = GETDATE();

   PRINT '>> Loading time :'+ CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)+' Seconds'

    PRINT '---------------------------------------------------------------------------'

	PRINT('>> Truncating table : silver.crm_sales_details')
    TRUNCATE TABLE silver.crm_sales_details
    PRINT('>> Inserting data into : silver.crm_sales_details')


     SET @start_time = GETDATE()
    INSERT INTO silver.crm_sales_details
(
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

SELECT 
	sls_ord_num ,
	sls_prd_key , 
	sls_cust_id ,
	
	CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN null
	     ELSE cast(cast(sls_order_dt as VARCHAR) as date)
	END as sls_order_dt,
	
	CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN null
	     ELSE cast(cast(sls_ship_dt as VARCHAR) AS date)
	END as sls_ship_dt,

	CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN null
	     ELSE cast(cast(sls_due_dt as VARCHAR) as date) 
    END  as sls_due_dt,

	
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	     THEN sls_quantity * sls_price
		 else sls_sales
	END as sls_sales, 

	sls_quantity ,

	CASE WHEN sls_price IS NULL OR sls_price <= 0 OR sls_price != sls_sales / ABS(sls_quantity)
	     THEN sls_sales / NULLIF(sls_quantity,0)
		 else sls_price
	END as sls_price

    from bronze.crm_sales_details
    ---SET end_time = GETDATE();

	PRINT '---------------------------------------------------------------------------------------------------'


	PRINT('>> Truncating table : silver.erp_cust_AZ12')
    TRUNCATE TABLE silver.erp_cust_AZ12
    PRINT('>> Inserting data into : silver.erp_cust_AZ12')

     SET @start_time = GETDATE()
	 INSERT INTO silver.erp_cust_AZ12
        (
          CID,
          bdate,
          gen
          )

SELECT 
      
       CASE 
           WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
           ELSE CID
       END  CID,
       
       CASE WHEN bdate > GETDATE() THEN NULL
       ELSE bdate
       END bdate,

       CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            else 'n/a'
       END gen
       from bronze.erp_cust_AZ12

       ---SET end_time = GETDATE();

	   PRINT '-----------------------------------------------------------------------------------'


       PRINT('>> Truncating table : silver.erp_loc_A101')
       TRUNCATE TABLE silver.erp_loc_A101
       PRINT('>> Inserting data into :silver.erp_loc_A101')
        SET @start_time = GETDATE()
	   INSERT INTO silver.erp_loc_A101
      (CID, cntry)
       SELECT 
        REPLACE(CID, '-', ''), 
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
             WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
             WHEN TRIM(cntry) = '' or TRIM(cntry) IS NULL THEN 'n/a'
             ELSE TRIM(cntry)
        END AS cntry


        from bronze.erp_loc_A101
        ---SET end_time = GETDATE();

PRINT'---------------------------------------------------------------------------'

       PRINT('>> Truncating table : silver.erp_px_cat_g1v2')
       TRUNCATE TABLE silver.erp_px_cat_g1v2
       PRINT('>> Inserting data into : silver.erp_px_cat_g1v2') 
 SET @start_time = GETDATE()
INSERT INTO silver.erp_px_cat_g1v2
(
        ID,
        category,
        sub_category,
        maintenance
)
SELECT 
        ID,
        category,
        sub_category,
        maintenance
    from bronze.erp_px_cat_g1v2
   ---SET end_time = GETDATE();
END TRY
        BEGIN CATCH 
                    PRINT('Error Occurred during inserting data into silver laye')
                    PRINT('Error message :' + ERROR_MESSAGE())
                    PRINT('Error Number  :' + CAST(ERROR_NUMBER() AS NVARCHAR))
                    PRINT('Error function :' + CAST(ERROR_STATE() AS NVARCHAR))
        END CATCH
END

SET @batch_end_time = GETDATE()

PRINT'>> Loading Time' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR)+' Seconds'

