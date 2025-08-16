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

-- ======================== crm_sales_details ============================

select 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

--check for invalid dates
select 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

select * 
from bronze.crm_sales_details 
where sls_order_dt > sls_ship_dt AND sls_order_dt > sls_due_dt

-- Check data consistency between Sales, Quantity and Price
-- >> Sales = Qunatity * Price
-- >> Values must be NOT NULL, zer or negative
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- ======================== erp_cust_az12 ============================
-- cid column will use as a foreign key
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

--Identify out-of-range dates
SELECT DISTINCT
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' OR bdate > GETDATE()

SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info )

-- Data Standarization and consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12


--========================== erp_loc_a101 =================================
SELECT 
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('UD', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END as cntry --Normalize and handling missing or blank country codes
FROM bronze.erp_loc_a101

-- Consistency of cid
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)

-- Data standarization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry