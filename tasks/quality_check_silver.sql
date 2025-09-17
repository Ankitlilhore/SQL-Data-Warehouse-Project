/*
-----------------------------------------------------------------------------------------
Quality check
-----------------------------------------------------------------------------------------
Script Purpose :
                 This script performs quality checks to validate the integrity, consistency,
                 and accuracy of the gold layer. These checks ensure:
                - uniqueness of surrogate in dimention tables.
                - Refrentail integrity between fact and dimension tables.
                - validation of relationships in the data model for analytical purpose.

Usage Notes : 
               - Run these chekc after data loading silver layer.
               - Investigate and resolve any descrepancies during the checks.
*/
---Quality Check

---check Null OR  duplicates in primary key
---Exceptation : No result

select prd_id,
       count(*)
       from silver.crm_prd_inf
       group by prd_id
       having count(*) > 1 or prd_id IS NULL
       
        
---check for unwanted spaces
---Exceptation : No result

select prd_nm
       from silver.crm_prd_inf
       where prd_nm != TRIM(prd_nm)

---check for null or negative number
---Excetation : No result

select prd_cost
       from silver.crm_prd_inf
       where prd_cost < 0 OR  prd_cost is null


--- Data Standardization and consistency
SELECT DISTINCT prd_line
from silver.crm_prd_inf

--- Check invalid date order
select * from
silver.crm_prd_inf
where prd_end_dt < prd_start_dt
