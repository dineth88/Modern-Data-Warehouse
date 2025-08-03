use DataWarehouse
-- ==================== crm_cust_info =======================
-- Check for null or duplicates in primary key (With window func)
select
* from (select 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM silver.crm_cust_info)t where flag_last = 1

-- check for unwanted spaces
select cst_firstname from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

--Check data consistency in gender field
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

select * from silver.crm_cust_info


-- ====================== crm_prd_info ====================
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

--Check for null or duplicate in primary key
select 
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 OR prd_id is NULL

--category_id
SELECT distinct id from bronze.erp_px_cat_g1v2

-- product key
SELECT sls_prd_key FROM bronze.crm_sales_details

-- Check for invalid date orders
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt