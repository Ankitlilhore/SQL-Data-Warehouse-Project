
/*
-----------------------------------------------------------------------------
DDL Script : Create Bronze Tables
-----------------------------------------------------------------------------

This script create tables in 'brinze' schema, dropping
existing tables if they already exist.

Whenever required to change the structure of tables , we 
can run this script
*/


IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info
GO

CREATE TABLE bronze.crm_cust_info(
	[cst_id] [int] NULL,
	[cst_key] [nvarchar](50) NULL,
	[cst_firstname] [nvarchar](50) NULL,
	[cst_lastname] [nvarchar](50) NULL,
	[cst_material_status] [nvarchar](50) NULL,
	[cst_gndr] [nvarchar](50) NULL,
	[cst_create_date] [date] NULL
);

IF OBJECT_ID('bronze.crm_prd_inf', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_inf
GO

CREATE TABLE bronze.crm_prd_inf(
	[prd_id] [int] NULL,
	[prd_key] [nvarchar](50) NULL,
	[prd_nm] [nvarchar](50) NULL,
	[prd_cost] [int] NULL,
	[prd_line] [nvarchar](50) NULL,
	[prd_start] [datetime] NULL,
	[prd_end_dt] [datetime] NULL
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details
GO

CREATE TABLE bronze.crm_sales_details(
	[sls_ord_num] [nvarchar](50) NULL,
	[sls_prd_key] [nvarchar](50) NULL,
	[sls_cust_id] [int] NULL,
	[sls_order_dt] [int] NULL,
	[sls_ship_dt] [int] NULL,
	[sls_due_dt] [int] NULL,
	[sls_sales] [int] NULL,
	[sls_quantity] [int] NULL,
	[sls_price] [int] NULL
);

IF OBJECT_ID('bronze.erp_cust_AZ12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_AZ12
GO

CREATE TABLE bronze.erp_cust_AZ12(
	[CID] [nvarchar](50) NULL,
	[bdate] [date] NULL,
	[GEN] [nvarchar](50) NULL
);

IF OBJECT_ID('bronze.erp_loc_A101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_A101
GO

CREATE TABLE bronze.erp_loc_A101(
	[CID] [nvarchar](50) NULL,
	[cntry] [nvarchar](50) NULL
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE bronze.erp_px_cat_g1v2
GO

CREATE TABLE bronze.erp_px_cat_g1v2(
	[ID] [nvarchar](50) NULL,
	[category] [nvarchar](50) NULL,
	[sub_category] [nvarchar](50) NULL,
	[maintenance] [nvarchar](50) NULL
);

