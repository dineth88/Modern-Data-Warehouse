INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
select
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
-- Map marital_status field into S --> Single and M --> Married
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'n/a' -- Adding default for null and other values
END cst_marital_status,
-- Map gender field into M --> Male and F --> Female
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a' -- Adding default for null and other values
END cst_gnder,
cst_create_date
from (select 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info)t where flag_last = 1
