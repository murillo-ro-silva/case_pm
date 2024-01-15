-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Case Questions
-- MAGIC
-- MAGIC 1. Create a dashboard or analysis using Excel that addresses the following:
-- MAGIC
-- MAGIC     1.a How could we improve the conversion rate from application initiation to policy
-- MAGIC     purchase? You are welcome to communicate these results however you would like, but at minimum the reader should be able to understand what the conversion rates look like and where the biggest drop offs are occuring.
-- MAGIC
-- MAGIC     1.b Based on 1a and the monthly premiums and policy lengths, what would be a reasonable acquisition cost target to give to the marketing team?
-- MAGIC
-- MAGIC 2. If you were given more time to work on this problem, what would you like to do? Is there any other data you would like to collect?
-- MAGIC Given the time limit, remember that we’re looking for you to display good communication skills and address the core business problems. Your dashboard / analysis should make it clear to the reader the answers to the questions above. You can use any combination of data, visualizations, text etc. to communicate your message!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Setup 
-- MAGIC
-- MAGIC Execute the following commands below Cmd3 and Cmd4.

-- COMMAND ----------

-- MAGIC %py
-- MAGIC !rm -rf case_policyme/ && git clone https://github.com/murillo-ro-silva/case_policyme.git

-- COMMAND ----------

-- DROP DATABASE policyme CASCADE;
CREATE DATABASE IF NOT EXISTS policyme;
CREATE TABLE IF NOT EXISTS policyme.insurance_events
  USING csv
  OPTIONS (path "file:/databricks/driver/case_policyme/analytics_engineer_take_home_assignment_v1.csv", header "true");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analysis and Resolution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1.a How could we improve the conversion rate from application initiation to policy purchase? You are welcome to communicate these results however you would like, but at minimum the reader should be able to understand what the conversion rates look like and where the biggest drop offs are occuring.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Approach to Analyze Question **1.a**
-- MAGIC
-- MAGIC My logical thinking behind it will be:
-- MAGIC
-- MAGIC 1. Calculate the conversion rate at each stage;
-- MAGIC 2. Identify at which stage the most significant drop-offs occur;
-- MAGIC 3. Identify the percentage of each step on the funnel to get a complementary perspective;
-- MAGIC 4. Create segmentation to understand the reasons behind the most significant drop-off.

-- COMMAND ----------

-- DBTITLE 1,drop-off unsuccessfully rate %
CREATE OR REPLACE TABLE absolute_metrics AS
SELECT
  -- Calculate total applications initiated
  COUNT(*) AS total_initiated,
  -- Calculate applications with a complete date
  COUNT(application_complete_date) AS total_completed,
  -- Calculate applications with an approval decision of 'Approved'
  COUNT(CASE WHEN application_approval_decision = 'Approved' THEN 1 END) AS total_approved,
  -- Calculate applications with a purchase date
  COUNT(CASE WHEN policy_purchase_date is not null THEN 1 END) AS total_purchased
FROM
  policyme.insurance_events
WHERE
  application_start_date is not null

-- COMMAND ----------

-- DBTITLE 1,drop-off unsuccessfully rate %
SELECT
  100 AS 1_aplication_initiated,
  100 - round(total_completed / total_initiated * 100,2) AS 2_initial_to_completion,
  100 - round(total_approved / total_completed * 100,2) AS 3_completion_aproval,
  100 - round(total_purchased / total_approved * 100,2) AS 4_aproval_to_purchased
FROM
  absolute_metrics

-- COMMAND ----------

-- DBTITLE 1,funnel successful rate # | %
SELECT
  '1_Initiated' AS step,
  total_initiated AS total
FROM absolute_metrics
UNION
SELECT
  '2_Completed' AS step,
  total_completed AS total
FROM absolute_metrics
UNION
SELECT
  '3_Approved' AS step,
  total_approved AS total
FROM absolute_metrics
UNION
SELECT
  '4_Purchased' AS step,
  total_purchased AS total
FROM absolute_metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Looking a little trying to get some details about the drop-off, I created queries to segment the users:
-- MAGIC
-- MAGIC * Status after Aproval and before Purchase
-- MAGIC * Age
-- MAGIC * Gender
-- MAGIC * Product Type
-- MAGIC * Lead Source
-- MAGIC

-- COMMAND ----------

-- Status after Aproval and before Purchase
SELECT
  purchase_status,
  COUNT(1) as total_policies_per
FROM 
  policyme.insurance_events, absolute_metrics a
WHERE
  policy_purchase_date is NULL AND
  application_approval_decision = 'Approved'
GROUP BY 
  purchase_status

-- COMMAND ----------

-- Segmentantion by demographic data (bucket age)
SELECT
  CASE 
    WHEN user_age >= 0 AND user_age < 10 THEN '0-9'
    WHEN user_age >= 10 AND user_age < 20 THEN '10-19'
    WHEN user_age >= 20 AND user_age < 30 THEN '20-29'
    WHEN user_age >= 30 AND user_age < 40 THEN '30-39'
    WHEN user_age >= 40 AND user_age < 50 THEN '40-49'
    WHEN user_age >= 50 AND user_age < 60 THEN '50-59'
    WHEN user_age >= 60 AND user_age < 70 THEN '60-69'
    WHEN user_age >= 70 AND user_age < 80 THEN '70-79'
    WHEN user_age >= 80 THEN '80+'
  ELSE 'Unknown' -- for any non-numeric or negative ages
  END AS age_bucket,
  COUNT(1) AS total_by_age
FROM 
  policyme.insurance_events
WHERE
  policy_purchase_date is NULL AND
  application_approval_decision = 'Approved'
GROUP BY age_bucket
ORDER BY age_bucket ASC

-- COMMAND ----------

-- Segmentantion by demographic data (age)
SELECT
  user_age,
  COUNT(1) AS total_by_age
FROM 
  policyme.insurance_events
WHERE
  policy_purchase_date is NULL AND
  application_approval_decision = 'Approved'
GROUP BY user_age

-- COMMAND ----------

-- Segmentantion by demographic data
SELECT
  user_gender,
  COUNT(1) AS total_by_gender
FROM 
  policyme.insurance_events
WHERE
  policy_purchase_date is NULL AND
  application_approval_decision = 'Approved'
GROUP BY user_gender
ORDER BY all

-- COMMAND ----------

-- Conversion rates by product type
SELECT
  product_type,
  COUNT(policy_number) AS policies_by_product
FROM 
  policyme.insurance_events
WHERE
  policy_purchase_date is NULL AND
  application_approval_decision = 'Approved'
GROUP BY product_type;

-- COMMAND ----------

-- Lead Source efficiency
SELECT
  lead_source,
  COUNT(policy_number) AS policies_by_source
FROM 
  policyme.insurance_events
WHERE
  policy_purchase_date is NULL AND
  application_approval_decision = 'Approved'
GROUP BY 
  lead_source
ORDER BY 
  policies_by_source DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **The Drop-off Vision (Cmd9):**
-- MAGIC
-- MAGIC * The initial stage to completion has a drop-off rate, which can be calculated as 100% - 49.14% = **50.86%**.
-- MAGIC * The completion to approval seems to have a better conversion rate so that the drop-off would be 100% - 57.56% = **42.44%**.
-- MAGIC * The final stage, approval to purchase, shows a drop-off of 100% - 34.4% = **65.6%**.
-- MAGIC
-- MAGIC **The Funnel Vision (Cmd10):**
-- MAGIC
-- MAGIC * The dataviz let clearly a complementary vision that show what percentage of leads / customers are converted to final purchase.
-- MAGIC
-- MAGIC ### Conclusion 1.a
-- MAGIC
-- MAGIC `Answer:` From these calculations, the most significant **drop-off occurs from the approval to purchase stage** with a **65.6% drop-off rate**. This indicates that even though customers get approved, many do not follow through with the purchase. This stage would likely benefit from further analysis to understand the reasons behind the drop-offs and to develop strategies to improve the final conversion rate.
-- MAGIC
-- MAGIC `looking at the highest dropout rate. (65.6% approval to purchase)`
-- MAGIC 1. There are 2 mainly situation preventing the policy from being sold in [Cmd12].
-- MAGIC | purchase_status   | total_policies_per_status |
-- MAGIC | ----------------- | ------------------------- |
-- MAGIC | Customer declined | 92  |
-- MAGIC | Pending           | 255 |
-- MAGIC
-- MAGIC 2. The behaviour occurs in an age group with a higher incidence in [Cmd13 and Cmd14].
-- MAGIC | top | bucket_age   | total_by_age |
-- MAGIC | --- | ------------ | ------------------------- |
-- MAGIC | 1   | 30-39        | 168  |
-- MAGIC | 2   | 40-49        | 89 |
-- MAGIC | 3   | 50-59        | 42 |
-- MAGIC | 4   | 20-29        | 42 |
-- MAGIC | 5   | 60-69        | 6 |
-- MAGIC
-- MAGIC 3. The higher incidence for people "Females" (181 events) than "Males" (166 events) in [Cmd15].
-- MAGIC 4. There was a higher incidence of product "Term Life" (328 events) than Critical Illness (19 events) in [Cmd16].
-- MAGIC 5. The top 5 lead sources for the events in [Cmd17].
-- MAGIC
-- MAGIC | top | lead_source   | policies_by_source |
-- MAGIC | --- | ------------ | ------------------------- |
-- MAGIC | 1   | Facebook Paid       | 105  |
-- MAGIC | 2   | Google Paid      | 79 |
-- MAGIC | 3   | Affiliate       | 61 |
-- MAGIC | 4   | SEO        | 45 |
-- MAGIC | 5   | Direct        | 20 |
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1.b Based on 1a and the monthly premiums and policy lengths, what would be a reasonable acquisition cost target to give to the marketing team?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Approach to Analyze Question **1.b**
-- MAGIC
-- MAGIC The logical thinking behind it will be:
-- MAGIC
-- MAGIC 1. Calculate the average lifetime value of a policy and its length.
-- MAGIC 2. Establish a reasonable acquisition cost based on the average lifetime value.
-- MAGIC     * Let's use the example of the company willing to spend 10% of LTV (Lifetime Value) on acquisition.
-- MAGIC

-- COMMAND ----------

WITH lifetime_value AS (
  SELECT
    AVG(policy_monthly_premiums) AS average_monthly_premium,
    AVG(policy_length_years) * 12 AS average_policy_length_months -- Assuming 12 payments per year
  FROM 
    policyme.insurance_events
  WHERE
    purchase_status = 'Purchased'
)

SELECT
  average_monthly_premium,
  average_policy_length_months,
  (average_monthly_premium * average_policy_length_months) AS estimated_ltv,
  -- Acquisition cost target could be a percentage of revenue per application or LTV, here we take a hypothetical 10% of the revenue per application
  (average_monthly_premium * average_policy_length_months * 0.10) AS acquisition_cost_target
FROM lifetime_value

-- COMMAND ----------

WITH lifetime_value AS (
  SELECT
    AVG(policy_monthly_premiums) AS average_monthly_premium,
    AVG(policy_length_years) * 12 AS average_policy_length_months, -- Assuming 12 payments per year
    (AVG(policy_monthly_premiums) * AVG(policy_length_years) * 12) estimated_ltv
  FROM 
    policyme.insurance_events
  WHERE 
    purchase_status = 'Purchased'
),

conversion_rate AS (
  SELECT
    COUNT(DISTINCT policy_number) * 1.0 / COUNT(DISTINCT record_id) AS conversion_rate
  FROM 
    policyme.insurance_events
  WHERE 
    application_complete_date IS NOT NULL -- Assuming application needs to be complete to be considered for conversion rate
)

SELECT
  ltv.average_monthly_premium,
  ltv.average_policy_length_months,
  (ltv.average_monthly_premium * ltv.average_policy_length_months) AS estimated_ltv,
  -- Adjusted acquisition cost target to consider conversion rate
  (ltv.estimated_ltv * cv.conversion_rate * 0.10) AS acquisition_cost_target
FROM lifetime_value ltv
CROSS JOIN conversion_rate cv; -- Cross join is used assuming there's only one row from each CTE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Some explanation about the CTEs above:
-- MAGIC
-- MAGIC 1. **Average Monthly Premiums and Policy Length**: Since these are related to policies, I should only calculate these averages for records where a policy has actually been purchased.
-- MAGIC 2. **Conversion Rate**: Conversion rate should be the number of unique policy numbers (policies purchased) divided by the total number of applications started.
-- MAGIC 3. **Acquisition Cost Target**: This should be calculated based on the estimated lifetime value (LTV) which is the product of the average monthly premium and the average policy length in months, and then factoring in the conversion rate.
-- MAGIC
-- MAGIC ### Conclusion 1.b
-- MAGIC
-- MAGIC `Answer:` The calculation of the acquisition cost target as 10% of the estimated LTV seems reasonable as a starting point. This percentage would depend on the industry standards, profit margins, and the company's strategy. The absolute number of 10% of cost target is 1116.05

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. If you were given more time to work on this problem, what would you like to do? Is there any other data you would like to collect?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Conclusion 2
-- MAGIC
-- MAGIC `Answer:`
-- MAGIC
-- MAGIC #### Approach to diving deep drop-off (some steps I probably will follow)
-- MAGIC
-- MAGIC Diving deeper into the reasons behind most drop-offs, we can create some approaches to help us find a better strategy to address.
-- MAGIC
-- MAGIC I'll create the methodology using all the steps or just one of them:
-- MAGIC
-- MAGIC ##### Step 1: Understanding deep "purchase_status" 
-- MAGIC * We need to understand which really mean status like "Customer declined" and "Pending", creating I draw to make the system owners visible or the business rule behind them.
-- MAGIC
-- MAGIC ##### Step 2: Different Drill-Down Analysis
-- MAGIC * Cohort Analysis: Examine the behavior of different cohorts over time.
-- MAGIC * Lead Source Efficiency: Assess the effectiveness of different lead sources in terms of conversion rates.
-- MAGIC * Premium Analysis: Investigate whether the policy monthly premiums correlate with drop-off rates.
-- MAGIC
-- MAGIC ##### Step 3: Qualitative Insights
-- MAGIC * Customer Feedback: Collect qualitative data through surveys or feedback forms from customers who dropped off.
-- MAGIC * Customer Service Logs: Review any available customer service logs for complaints or issues raised that might be related to drop-offs.
-- MAGIC
-- MAGIC ##### Step 4: Hypothesis Generation
-- MAGIC * Formulate Hypotheses: Based on the quantitative and qualitative data, formulate hypotheses on why drop-offs occur at certain stages.
-- MAGIC
-- MAGIC ##### Step 5: Testing & Validation
-- MAGIC * A/B Testing: If possible, perform A/B testing to validate the hypotheses.
-- MAGIC * Predictive Modeling: We use statistical models to predict drop-offs and validate findings against actual data.
-- MAGIC
-- MAGIC ##### Step 6: Actionable Insights
-- MAGIC * Insight Reporting: Compile a report with actionable insights and recommendations.
-- MAGIC * Strategic Recommendations: Propose specific changes or strategies to reduce drop-offs, such as improving the user experience, adjusting pricing, or enhancing follow-up communications.
-- MAGIC
-- MAGIC ##### Step 7: Monitoring
-- MAGIC * Implement Changes: Implement the proposed changes based on the insights.
-- MAGIC * Continuous Monitoring: Keep monitoring the conversion rates and customer feedback to assess the impact of the changes.
-- MAGIC
-- MAGIC ##### Step 8: Iterative Improvement
-- MAGIC * Iterative Analysis: We need regularly revisit the analysis to find new insights and continuously improve the conversion rates.
-- MAGIC
