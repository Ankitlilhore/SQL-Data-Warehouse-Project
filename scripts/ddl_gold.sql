/*

---------------------------------------------------------------------------------------------

DDL_Script : Create Gold Views
---------------------------------------------------------------------------------------------
  Script Purpose : 

  This script create views for the Gold layer in the data warehouse.
  The gold layer represents the final dimension and fact tables (Star Schema)

  Each view perform transformation and combines data from the silver layer
  to produce a clean, enriched and business ready datasets.

Usage: 
         These views can be queried directly for analytics and reporting
--------------------------------------------------------------------------------------------
*/

--------------------------------------------------------------------------------------------

 ---    Create Dimension : gold.dim_customers

--------------------------------------------------------------------------------------------       

CREATE VIEW gold.dim_customers AS
SELECT 
       ROW_NUMBER() OVER(ORDER BY ci.cst_id) as customer_key,
       ci.cst_id as customer_id,
       ci.cst_key as customer_number,
       ci.cst_firstname as first_name,
       ci.cst_lastname as last_name,
       ci.cst_material_status as marital_status,
       CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
            ELSE COALESCE(li.gen, 'n/a')
            end as  gender,
       li.bdate as birthdate,
       ci.cst_create_date as create_date,
       ll.cntry as country
       
       from silver.crm_cust_info ci
       LEFT JOIN  silver.erp_cust_AZ12 li
       ON ci.cst_key = li.CID
       LEFT JOIN silver.erp_loc_A101 ll
       ON ci.cst_key = ll.CID


-------------------------------------------------------------------------------------

--- Create Dimension : gold.dim_products

-------------------------------------------------------------------------------------

CREATE VIEW gold.dim_products as
SELECT
      ROW_NUMBER() OVER(order by prd_start_dt, prd_key) as product_key,
      pn.prd_id as product_id,
      pn.prd_key as product_number,
      pn.prd_nm as product_name,
      pn.cat_id as category_id,
      pc.category,
      pc.sub_category,
      pc.maintenance,
      pn.prd_cost as cost,
      pn.prd_line as product_line,
      pn.prd_start_dt as start_date

    
      from silver.crm_prd_inf pn
      LEFT JOIN silver.erp_px_cat_g1v2  pc
      ON pn.cat_id = pc.ID
      where prd_end_dt IS NULL


----------------------------------------------------------------------------------------------

--- Create Facts : gold.fact_sales

----------------------------------------------------------------------------------------------


CREATE VIEW gold.fact_sales as
select 
       sd.sls_ord_num as order_number,
       pr.product_key,
       cu.customer_key,
       sd.sls_order_dt as order_date,
       sd.sls_ship_dt as shipping_date,
       sd.sls_due_dt as due_date,
       sd.sls_sales as sales_amount,
       sd.sls_quantity as quantity,
       sd.sls_price as price
 
      from silver.crm_sales_details sd
      LEFT JOIN gold.dim_products pr
      ON sd.sls_prd_key = pr.product_number
      LEFT JOIN gold.dim_customers cu
      ON sd.sls_cust_id = cu.customer_id
     

   
       

