-- ===================================================================
-- CUSTOMER SEGMENTATION & SALES ATTRIBUTION ANALYSIS
-- ===================================================================
-- Shopify E-Commerce Marketing Analytics
-- Dataset: Orders + Attribution + Customer Dimensions
-- Database: Google BigQuery
-- Author: Vrushabh Malgatte
-- ===================================================================

-- ===================================================================
-- SECTION 1: DATA EXPLORATION & VALIDATION
-- ===================================================================

-- 1.1 Row counts per table (sense check for completeness)
SELECT 
  'orders' AS table_name, 
  COUNT(*) AS row_count  
FROM `projectspractice.marketing_data.orders`
UNION ALL
SELECT 
  'dimAttribution',
  COUNT(*)
FROM `projectspractice.marketing_data.dimAttribution`
UNION ALL
SELECT 
  'dimCustomer',
  COUNT(*)  
FROM `projectspractice.marketing_data.dimCustomer`;

-- 1.2 Preview data from each table
SELECT * FROM `projectspractice.marketing_data.orders` LIMIT 5;
SELECT * FROM `projectspractice.marketing_data.dimAttribution` LIMIT 5;
SELECT * FROM `projectspractice.marketing_data.dimCustomer` LIMIT 5;

-- 1.3 Order amount distribution (check for negative/zero values)
SELECT
  COUNT(DISTINCT orderNumber) AS total_orders,
  COUNTIF(orderTotalAmount IS NULL) AS missing_total_amount,
  AVG(orderTotalAmount) AS avg_total_amount,
  MIN(orderTotalAmount) AS min_total_amount,
  MAX(orderTotalAmount) AS max_total_amount
FROM `projectspractice.marketing_data.prep`;

-- 1.4 Refund amounts analysis
SELECT
  COUNTIF(orderRefundAmount <> 0) AS refunded_orders,
  SUM(orderRefundAmount) AS total_refunded
FROM `projectspractice.marketing_data.prep`;

-- 1.5 Date range (ensure enough data for cohort analysis)
SELECT
  MIN(orderDate) AS first_order,
  MAX(orderDate) AS last_order
FROM `projectspractice.marketing_data.prep`;

-- 1.6 Orders by month (identify seasonality/gaps)
SELECT
  EXTRACT(YEAR FROM orderDate) AS year,
  EXTRACT(MONTH FROM orderDate) AS month,
  COUNT(DISTINCT orderNumber) AS orders_count
FROM `projectspractice.marketing_data.prep`
GROUP BY year, month
ORDER BY year, month;

-- 1.7 Order status distribution (how many are actually completed?)
SELECT
  orderStatus,
  COUNT(DISTINCT orderNumber) AS count
FROM `projectspractice.marketing_data.prep`
GROUP BY orderStatus;

-- 1.8 Payment method distribution
SELECT
  paymentMethod,
  COUNT(DISTINCT orderNumber) AS count
FROM `projectspractice.marketing_data.prep`
GROUP BY paymentMethod;

-- 1.9 Country distribution (concentration check)
SELECT
  countryCode,
  COUNT(DISTINCT orderNumber) AS transaction_count,
  SUM(COUNT(DISTINCT orderNumber)) OVER() AS total_transactions,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentage_of_total
FROM `projectspractice.marketing_data.prep`
GROUP BY countryCode
ORDER BY transaction_count DESC;

-- 1.10 Attribution channel distribution (before cleaning)
SELECT
  attributionSource,
  COUNT(DISTINCT orderNumber) AS count
FROM `projectspractice.marketing_data.prep`
GROUP BY attributionSource
ORDER BY count DESC;

-- 1.11 Attribution device distribution (before cleaning)
SELECT
  attributionDevice,
  COUNT(DISTINCT orderNumber) AS count
FROM `projectspractice.marketing_data.prep`
GROUP BY attributionDevice
ORDER BY count DESC;

-- 1.12 Null orders check (missing customer or attribution)
SELECT
  SUM(CASE WHEN c.orderNumber IS NULL THEN 1 ELSE 0 END) AS no_customer,
  SUM(CASE WHEN a.orderNumber IS NULL THEN 1 ELSE 0 END) AS no_attribution
FROM `projectspractice.marketing_data.prep` o
LEFT JOIN `projectspractice.marketing_data.dimCustomer` c USING(orderNumber)
LEFT JOIN `projectspractice.marketing_data.dimAttribution` a USING(orderNumber);

-- ===================================================================
-- SECTION 2: CREATE UNIFIED PREP TABLE
-- ===================================================================
-- Join all 3 tables on orderNumber to create single transactional view

CREATE OR REPLACE TABLE `projectspractice.marketing_data.prep` AS
SELECT
  o.*,
  c.customerId,
  a.attributionDevice,
  a.attributionSource
FROM `projectspractice.marketing_data.orders` AS o
JOIN `projectspractice.marketing_data.dimCustomer` AS c ON o.orderNumber = c.orderNumber
JOIN `projectspractice.marketing_data.dimAttribution` AS a ON o.orderNumber = a.orderNumber;

-- ===================================================================
-- SECTION 3: DATA CLEANING & NORMALIZATION
-- ===================================================================

-- 3.1 Create first clean view: filter for positive order amounts
CREATE OR REPLACE VIEW `projectspractice.marketing_data.v_cleaned_orders` AS
SELECT *
FROM `projectspractice.marketing_data.prep`
WHERE orderTotalAmount >= 0;

-- 3.2 Profile cleaned orders
SELECT
  COUNT(DISTINCT orderNumber) AS total_orders,
  AVG(orderTotalAmount) AS AOV,
  SUM(orderTotalAmount) AS total_revenue
FROM `projectspractice.marketing_data.v_cleaned_orders`;

-- 3.3 Fix attributionDevice: extract first value (remove duplicates like "desktop,mobile")
CREATE OR REPLACE VIEW `projectspractice.marketing_data.v_cleaned_orders_2` AS
SELECT
  o.orderNumber,
  o.orderStatus,
  o.orderDate,
  o.countryCode,
  o.paymentMethod,
  o.orderRefundAmount,
  o.orderTotalAmount,
  o.itemName,
  o.quantity,
  o.attributionSource,
  o.customerId,
  REGEXP_EXTRACT(o.attributionDevice, r'^([^,]+)') AS attributionDevice
FROM `projectspractice.marketing_data.v_cleaned_orders` o;

-- Check distinct values after cleaning
SELECT DISTINCT attributionDevice FROM `projectspractice.marketing_data.v_cleaned_orders_2`;

-- 3.4 Fix attributionSource: extract first value (remove duplicates)
CREATE OR REPLACE VIEW `projectspractice.marketing_data.v_cleaned_orders_3` AS
SELECT
  o.orderNumber,
  o.orderStatus,
  o.orderDate,
  o.countryCode,
  o.paymentMethod,
  o.orderRefundAmount,
  o.orderTotalAmount,
  o.itemName,
  o.quantity,
  o.attributionDevice,
  o.customerId,
  REGEXP_EXTRACT(o.attributionSource, r'^([^,]+)') AS attributionSource
FROM `projectspractice.marketing_data.v_cleaned_orders_2` o;

-- Check distinct values after cleaning
SELECT DISTINCT attributionSource FROM `projectspractice.marketing_data.v_cleaned_orders_3`;

-- 3.5 Normalize payment method and filter to completed orders
CREATE OR REPLACE VIEW `projectspractice.marketing_data.v_cleaned_orders_4` AS
SELECT
  o.* REPLACE (
    CASE
      WHEN LOWER(o.paymentMethod) LIKE '%later' THEN 'Unknown'
      WHEN LOWER(o.paymentMethod) LIKE '%card%' THEN 'Card'
      WHEN LOWER(o.paymentMethod) LIKE '%paypal%' THEN 'PayPal'
      ELSE o.paymentMethod
    END AS paymentMethod
  )
FROM `projectspractice.marketing_data.v_cleaned_orders_3` AS o
WHERE orderStatus = "Completed";

-- Validate payment method distribution after normalization
SELECT
  DISTINCT paymentMethod,
  COUNT(*) as number
FROM `projectspractice.marketing_data.v_cleaned_orders_4`
GROUP BY 1;

-- ===================================================================
-- SECTION 4: CREATE ANALYTICAL VIEWS
-- ===================================================================

-- 4.1 Revenue by Channel
CREATE OR REPLACE VIEW `projectspractice.marketing_data.v_revenue_by_channel` AS
SELECT
  attributionSource,
  COUNT(DISTINCT orderNumber) AS ordersCount,
  SUM(orderTotalAmount) AS totalRev,
  AVG(orderTotalAmount) AS AOV
FROM `projectspractice.marketing_data.v_cleaned_orders_4`
GROUP BY attributionSource
ORDER BY totalRev DESC;

-- Query to view results
SELECT * FROM `projectspractice.marketing_data.v_revenue_by_channel`;

-- 4.2 Customer Lifetime Value (LTV)
CREATE OR REPLACE VIEW `projectspractice.marketing_data.v_customer_ltv` AS
SELECT
  customerId,
  COUNT(DISTINCT orderNumber) AS totalOrders,
  SUM(orderTotalAmount) AS totalSpend,
  MIN(orderDate) AS firstOrder,
  MAX(orderDate) AS lastOrder,
  DATE_DIFF(MAX(orderDate), MIN(orderDate), DAY) AS customerLifespanDays,
  SAFE_DIVIDE(SUM(orderTotalAmount), COUNT(DISTINCT orderNumber)) AS AOV
FROM `projectspractice.marketing_data.v_cleaned_orders_4`
GROUP BY customerId;

-- ===================================================================
-- SECTION 5: RFM SEGMENTATION (Recency, Frequency, Monetary)
-- ===================================================================
-- Note: Due to ties in NTILE, RFM segment counts may vary slightly between runs
-- This is expected behavior with tied values

CREATE OR REPLACE VIEW `projectspractice.marketing_data.v_customer_rfm` AS
WITH base AS (
  SELECT
    customerId,
    MAX(orderDate) AS lastOrder,
    COUNT(DISTINCT orderNumber) AS frequency,
    SUM(orderTotalAmount) AS monetary
  FROM `projectspractice.marketing_data.v_cleaned_orders_4`
  GROUP BY customerId
),
recencyCalc AS (
  SELECT
    customerId,
    DATE_DIFF(DATE('2025-08-06'), DATE(lastOrder), DAY) AS recencyDays,
    frequency,
    monetary
  FROM base
)
SELECT
  customerId,
  recencyDays,
  frequency,
  monetary,
  NTILE(4) OVER (ORDER BY recencyDays ASC) AS rQuartile,
  NTILE(4) OVER (ORDER BY frequency DESC) AS fQuartile,
  NTILE(4) OVER (ORDER BY monetary DESC) AS mQuartile
FROM recencyCalc;

-- View RFM segment distribution
SELECT
  CONCAT('R', rQuartile, 'F', fQuartile, 'M', mQuartile) AS rfmSegment,
  COUNT(*) AS customers
FROM `projectspractice.marketing_data.v_customer_rfm`
GROUP BY rfmSegment
ORDER BY rfmSegment;

-- ===================================================================
-- SECTION 6: COHORT RETENTION ANALYSIS
-- ===================================================================

CREATE OR REPLACE VIEW `projectspractice.marketing_data.v_cohort_retention` AS
WITH customerCohorts AS (
  SELECT
    customerId,
    MIN(DATE_TRUNC(DATE(orderDate), MONTH)) AS cohortMonth
  FROM `projectspractice.marketing_data.v_cleaned_orders_4`
  GROUP BY customerId
),
ordersFull AS (
  SELECT
    o.customerId,
    DATE_TRUNC(DATE(o.orderDate), MONTH) AS orderMonth,
    c.cohortMonth
  FROM `projectspractice.marketing_data.v_cleaned_orders_4` AS o
  JOIN customerCohorts AS c
    ON CAST(o.customerId AS INT64) = c.customerId
),
cohortPrep AS (
  SELECT
    customerId,
    cohortMonth,
    orderMonth,
    DATE_DIFF(orderMonth, cohortMonth, MONTH) AS monthsSinceAcq
  FROM ordersFull
)
SELECT
  cohortMonth,
  monthsSinceAcq,
  COUNT(DISTINCT customerId) AS activeCustomers
FROM cohortPrep
GROUP BY cohortMonth, monthsSinceAcq
ORDER BY cohortMonth, monthsSinceAcq;

-- ===================================================================
-- SECTION 7: ANALYSIS - CHANNEL PERFORMANCE
-- ===================================================================

-- 7.1 Total revenue by attribution source (concentration analysis)
SELECT
  attributionSource,
  SUM(totalRev) AS totalRevenue,
  ROUND(100 * SUM(totalRev) / SUM(SUM(totalRev)) OVER(), 2) AS pct_of_total
FROM `projectspractice.marketing_data.v_revenue_by_channel`
GROUP BY attributionSource
ORDER BY totalRevenue DESC;

-- ===================================================================
-- SECTION 8: ANALYSIS - CUSTOMER VALUE DISTRIBUTION
-- ===================================================================

-- 8.1 Customer LTV by quintile (Pareto pattern check)
WITH clv_ranked AS (
  SELECT
    customerId,
    totalSpend,
    NTILE(5) OVER (ORDER BY totalSpend) AS spendQuintile
  FROM `projectspractice.marketing_data.v_customer_ltv`
)
SELECT
  spendQuintile,
  COUNT(*) AS customers,
  AVG(totalSpend) AS avgSpend
FROM clv_ranked
GROUP BY spendQuintile
ORDER BY spendQuintile;

-- ===================================================================
-- SECTION 9: ANALYSIS - RFM SEGMENT COUNTS
-- ===================================================================

SELECT
  CONCAT('R', rQuartile, 'F', fQuartile, 'M', mQuartile) AS rfmSegment,
  COUNT(*) AS customers
FROM `projectspractice.marketing_data.v_customer_rfm`
GROUP BY rfmSegment
ORDER BY rfmSegment;

-- ===================================================================
-- SECTION 10: ANALYSIS - TOP PRODUCTS BY CUSTOMER ARCHETYPE
-- ===================================================================

WITH order_rfm AS (
  SELECT
    o.orderNumber,
    o.itemName,
    o.orderTotalAmount,
    r.customerId,
    r.rQuartile,
    r.fQuartile,
    r.mQuartile
  FROM `projectspractice.marketing_data.v_cleaned_orders_4` AS o
  JOIN `projectspractice.marketing_data.v_customer_rfm` AS r
    ON o.customerId = r.customerId
),
archetyped AS (
  SELECT
    itemName,
    orderTotalAmount,
    CASE
      WHEN rQuartile = 1 AND fQuartile = 1 AND mQuartile IN (1,2) THEN 'Champions'
      WHEN rQuartile IN (1,2) AND fQuartile IN (1,2) AND mQuartile IN (2,3) THEN 'Loyal'
      WHEN rQuartile = 1 AND fQuartile IN (2,3) AND mQuartile IN (2,3) THEN 'Potential Loyalists'
      WHEN rQuartile = 1 AND fQuartile = 4 AND mQuartile = 4 THEN 'New Customers'
      WHEN rQuartile IN (3,4) AND fQuartile IN (1,2) AND mQuartile IN (1,2) THEN 'At Risk Loyal'
      WHEN rQuartile = 4 AND fQuartile IN (3,4) AND mQuartile IN (3,4) THEN 'Hibernating'
      ELSE 'Other'
    END AS rfm_archetype
  FROM order_rfm
),
agg AS (
  SELECT
    rfm_archetype,
    itemName,
    SUM(orderTotalAmount) AS revenue
  FROM archetyped
  GROUP BY rfm_archetype, itemName
),
ranked AS (
  SELECT
    rfm_archetype,
    itemName,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY rfm_archetype ORDER BY revenue DESC) AS rn,
    SUM(revenue) OVER (PARTITION BY rfm_archetype) AS archetype_total,
    ROUND(100 * revenue / SUM(revenue) OVER (PARTITION BY rfm_archetype), 2) AS pct_within_archetype,
    ROUND(100 * revenue / SUM(revenue) OVER (), 2) AS pct_of_total
  FROM agg
)
SELECT
  rfm_archetype,
  itemName AS top_product,
  revenue,
  archetype_total,
  pct_within_archetype,
  pct_of_total
FROM ranked
WHERE rn <= 5
ORDER BY rfm_archetype, revenue DESC;

-- ===================================================================
-- SECTION 11: REPEAT PURCHASE ANALYSIS
-- ===================================================================

-- 11.1 Repeat purchase rate
SELECT
  COUNT(DISTINCT CASE WHEN totalOrders > 1 THEN customerId END) AS repeat_customers,
  COUNT(DISTINCT customerId) AS total_customers,
  ROUND(100 * COUNT(DISTINCT CASE WHEN totalOrders > 1 THEN customerId END) 
    / COUNT(DISTINCT customerId), 2) AS repeat_purchase_rate
FROM `projectspractice.marketing_data.v_customer_ltv`;

-- ===================================================================
-- KEY INSIGHTS & RECOMMENDATIONS
-- ===================================================================

/*
FINDINGS FROM ANALYSIS:

1. CHANNEL PERFORMANCE:
   - Direct: 42.39% (highest, concentration risk)
   - YouTube: 29.72% (strong secondary)
   - Google: 14.95% (consistent)
   - Top 3 channels = 87% of sales

2. CUSTOMER VALUE:
   - Pareto pattern: Q5 spends 47x more than Q1
   - Only 5.33% of customers are repeat purchasers
   - Very few make a second purchase within 90 days

3. RFM ARCHETYPES:
   - Champions: 14 customers, high value, retention priority
   - Hibernating: Valuable past customers, win-back opportunity
   - At Risk: Were loyal, now inactive
   - New: Low frequency/spend, nurture needed

4. PRODUCT-SEGMENT FIT:
   - Champions: Premium products (60% of segment)
   - Hibernating: Entry-level products (win-back lever)
   - Cross-sell varies by archetype

RECOMMENDATIONS:
   1. Build post-purchase email flows (Days 0-60) to lift repeat rate
   2. Create early-access programs for Champions
   3. Tailor homepage/merchandising by RFM segment
   4. Test underutilized channels to reduce Direct dependency
   5. Implement win-back campaigns for Hibernating segment
*/
