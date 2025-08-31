

-- =================gold.dim_customers ============================
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- Checking for primary key duplicates
SELECT cst_id, COUNT(*) FROM
(	SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci 
	LEFT JOIN silver.erp_cust_az12 ca -- Master table LEFT JOIN Table A --> Need all records in master table
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*) > 1

-- Data intergration for two fields cst_gndr and gen
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender info
		ELSE COALESCE(ca.gen, 'n/a') -- COALESCE --> returns the first not-null value in a list
	END AS new_gen
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1, 2

-- ================== gold.dim_products ============================

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data

-- Check the uniqueness
-- Product key
SELECT prd_key, COUNT(*) FROM (
SELECT
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id,
	pc.cat,
	pc.subcat,
	pc.maintenance,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL 
)t GROUP BY prd_key
HAVING COUNT(*) > 1

SELECT * FROM gold.dim_products

-- ======================== gold.sales ===============================
-- This fact tables is connecting mutiple dimensions through sarrogate kleys comming from dimensions
--    _ _ _ _ _ _               _ _ _ _ _ _               _ _ _ _ _ _ 
--   |           |    Key      |           |    Key      |           |
--   |    Dim    |- - - - - - -|  Fact     |- - - - - - -|    Dim    |
--   |_ _ _ _ _ _|             |_ _ _ _ _ _|             |_ _ _ _ _ _| 

--   Data Lookup : Joining the dimension tables in order to get their sarrogate keys
DROP VIEW gold.fact_sales

CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num AS order_number,
cu.customer_key,
sd.sls_cust_id,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
ON sd.sls_cust_id = cu.customer_id

SELECT * FROM gold.fact_sales

SELECT * FROM silver.crm_sales_details