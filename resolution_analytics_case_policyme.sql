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
-- MAGIC !git clone https://github.com/murillo-ro-silva/case_policyme.git

-- COMMAND ----------

DROP DATABASE policyme CASCADE;
CREATE DATABASE policyme;
CREATE TABLE policyme.insurance_events
  USING csv
  OPTIONS (path "file:/databricks/driver/case_policyme/analytics_engineer_take_home_assignment_v1.csv", header "true");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analysis and Resolution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1.a To improve the conversion rate from application initiation to policy purchase, analyze the data at each stage of the customer journey. Look for patterns such as drop-off points or stages with higher rejection rates. Consider the following

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Steps to help Analyze question **1.a**
-- MAGIC
-- MAGIC The logical thinking behind it will be:
-- MAGIC
-- MAGIC 1. Calculate the conversion rate at each stage.
-- MAGIC 2. Identify at which stage the biggest drop-offs occur.
-- MAGIC 3. Identify the percentage of each step on the funnel.

-- COMMAND ----------

-- DBTITLE 1,drop-off unsuccessfully rate %
WITH absolute_values as (
  SELECT
    -- Calculate total applications initiated
    COUNT(*) AS total_initiated,

    -- Calculate applications with a complete date
    COUNT(application_complete_date) AS total_completed,

    -- Calculate applications with an approval decision of 'Approved'
    COUNT(CASE WHEN application_approval_decision = 'Approved' THEN 1 END) AS total_approved,

    -- Calculate applications with a purchase date
    COUNT(policy_purchase_date) AS total_purchased
  FROM
    policyme.insurance_events
  WHERE
    application_start_date is not null
)

SELECT
  100 AS 1_aplication_initiated,
  100 - round(total_completed / total_initiated * 100,2) AS 2_initial_to_completion,
  100 - round(total_approved / total_completed * 100,2) AS 3_completion_aproval,
  100 - round(total_purchased / total_approved * 100,2) AS 4_aproval_to_purchased
FROM
  absolute_values

-- COMMAND ----------

-- DBTITLE 1,funnel successful rate # | %
WITH absolute_values as (
  SELECT
    -- Calculate total applications initiated
    COUNT(*) AS total_initiated,

    -- Calculate applications with a complete date
    COUNT(application_complete_date) AS total_completed,

    -- Calculate applications with an approval decision of 'Approved'
    COUNT(CASE WHEN application_approval_decision = 'Approved' THEN 1 END) AS total_approved,

    -- Calculate applications with a purchase date
    COUNT(policy_purchase_date) AS total_purchased
  FROM
    policyme.insurance_events
  WHERE
    application_start_date is not null
)

SELECT
  '1_Initiated' AS step,
  total_initiated AS total
FROM absolute_values
UNION
SELECT
  '2_Completed' AS step,
  total_completed AS total
FROM absolute_values
UNION
SELECT
  '3_Approved' AS step,
  total_approved AS total
FROM absolute_values
UNION
SELECT
  '4_Purchased' AS step,
  total_purchased AS total
FROM absolute_values

-- COMMAND ----------

-- MAGIC %md
-- MAGIC On the dashboard and query above, I address the following steps:
-- MAGIC
-- MAGIC 1. Calculate the conversion rate at each stage.
-- MAGIC 2. Identify at which stage the most significant drop-offs occur.
-- MAGIC 3. Identify the percentage of each step on the funnel.
-- MAGIC
-- MAGIC
-- MAGIC **The Drop-off Vision:**
-- MAGIC
-- MAGIC * The initial stage to completion has a drop-off rate, which can be calculated as 100% - 49.14% = **50.86%**.
-- MAGIC * The completion to approval seems to have a better conversion rate so that the drop-off would be 100% - 57.56% = **42.44%**.
-- MAGIC * The final stage, approval to purchase, shows a drop-off of 100% - 34.4% = **65.6%**.
-- MAGIC
-- MAGIC From these calculations, the most significant **drop-off occurs from the approval to the purchase stage** with a **65.6% drop-off rate**. 
-- MAGIC
-- MAGIC This indicates that even though customers get approved, many do not follow through with the purchase. This stage would likely benefit from further analysis to understand the reasons behind the drop-offs and to develop strategies to improve the final conversion rate.
-- MAGIC
-- MAGIC
-- MAGIC **The Funnel Vision:**
-- MAGIC
-- MAGIC * The dataviz let clearly a complementary vision that show what percentage of leads / customers are converted to final purchase.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Looking a little trying to get some details about the drop-off, I create queries to segmentation the users:
-- MAGIC
-- MAGIC * Age
-- MAGIC * Gender
-- MAGIC * Product Type
-- MAGIC * Lead Source
-- MAGIC

-- COMMAND ----------

-- Segmentantion by demographic data
SELECT
  user_age,
  COUNT(1) AS total_by_age
FROM 
  policyme.insurance_events
WHERE
  policy_purchase_date is NULL
GROUP BY user_age
ORDER BY all

-- COMMAND ----------

-- Segmentantion by demographic data
SELECT
  user_gender,
  COUNT(1) AS total_by_gender
FROM 
  policyme.insurance_events
WHERE
  policy_purchase_date is NULL
GROUP BY user_gender
ORDER BY all

-- COMMAND ----------

-- Segmentantion by demographic data
SELECT
  user_age,
  user_gender,
  COUNT(*) AS total_by_age_gender,
  COUNT(policy_number) AS policies_by_age_gender
FROM 
  policyme.insurance_events
GROUP BY user_age, user_gender;

-- COMMAND ----------

-- Conversion rates by product type
SELECT
  product_type,
  COUNT(1) AS total_by_product,
  COUNT(policy_number) AS policies_by_product
FROM 
  policyme.insurance_events
GROUP BY product_type;

-- COMMAND ----------

-- Lead Source efficiency
SELECT
  lead_source,
  COUNT(*) AS leads_by_source,
  COUNT(policy_number) AS policies_by_source
FROM 
  policyme.insurance_events
GROUP BY 
  lead_source
ORDER BY 
  leads_by_source DESC,
  policies_by_source DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1.b Based on 1a and the monthly premiums and policy lengths, what would be a reasonable acquisition cost target to give to the marketing team?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Steps to help Analyze question **1.b**
-- MAGIC
-- MAGIC The logical thinking behind it will be:
-- MAGIC
-- MAGIC 1. Calculate the average lifetime value of a policy and lenght.
-- MAGIC 2. Establish a reasonable acquisition cost based on the average lifetime value.
-- MAGIC     * Let's use the example the company willing to spend 10% of LTV (Lifetime Value) on acquisition.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Some explanation about the CTEs above:
-- MAGIC
-- MAGIC 1. Average Monthly Premium: The average amount customers pay per month.
-- MAGIC 2. Policy Length: The average duration customers are expected to keep their policies.
-- MAGIC 3. Conversion Rate: The percentage of initiated applications that result in a purchase.

-- COMMAND ----------

WITH lifetime_value AS (
  SELECT
    AVG(policy_monthly_premiums) AS average_monthly_premium,
    AVG(policy_length_years) * 12 AS average_policy_length_months -- Assuming 12 payments per year
  FROM policyme.insurance_events
  WHERE Policy_Number IS NOT NULL -- Considering only purchased policies
),

conversion_rate AS (
  SELECT
    (COUNT(policy_number) * 1.0 / COUNT(*)) AS conversion_rate
  FROM policyme.insurance_events
)

SELECT
  average_monthly_premium,
  average_policy_length_months,
  conversion_rate,
  (average_monthly_premium * average_policy_length_months) AS estimated_ltv,
  (average_monthly_premium * average_policy_length_months * conversion_rate) AS revenue_per_application,
  -- Acquisition cost target could be a percentage of revenue per application or LTV, here we take a hypothetical 10% of the revenue per application
  (average_monthly_premium * average_policy_length_months * conversion_rate * 0.10) AS acquisition_cost_target
FROM lifetime_value, conversion_rate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. If you were given more time to work on this problem, what would you like to do? Is there any other data you would like to collect?
-- MAGIC Given the time limit, remember that we’re looking for you to display good communication skills and address the core business problems. Your dashboard / analysis should make it clear to the reader the answers to the questions above. You can use any combination of data, visualizations, text etc. to communicate your message!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Approach to diving deep drop-off (some steps I probably will following)
-- MAGIC
-- MAGIC Diving deeper into the reasons behind most drop-offs, we can create some approaches to help us find a better strategy to address.
-- MAGIC
-- MAGIC I suggest we create the approach using all the steps or just one of them:
-- MAGIC
-- MAGIC ##### Step 1: Detailed Examination
-- MAGIC * Segmentation: Break down the data by various attributes like demographics (age, gender), income, product type, policy length, coverage amount, and lead source.
-- MAGIC * Trend Analysis: Look for trends over time to see if drop-offs are consistent or if they fluctuate during certain periods.
-- MAGIC
-- MAGIC ##### Step 2: Drill-Down Analysis
-- MAGIC * Cohort Analysis: Examine the behavior of different cohorts over time.
-- MAGIC * Lead Source Efficiency: Assess the effectiveness of different lead sources in terms of conversion rates.
-- MAGIC * Premium Analysis: Investigate whether the policy monthly premiums correlate with drop-off rates.
-- MAGIC
-- MAGIC ##### Step 4: Funnel Analysis
-- MAGIC * Funnel Visualization: Create a funnel visualization to clearly see how many prospects drop off at each stage.
-- MAGIC * Leakage Points: Use the visualization to identify the specific points in the funnel where the most significant drop-offs occur.
-- MAGIC
-- MAGIC ##### Step 5: Qualitative Insights
-- MAGIC * Customer Feedback: Collect qualitative data through surveys or feedback forms from customers who dropped off.
-- MAGIC * Customer Service Logs: Review any available customer service logs for complaints or issues raised that might be related to drop-offs.
-- MAGIC
-- MAGIC ##### Step 6: Hypothesis Generation
-- MAGIC * Formulate Hypotheses: Based on the quantitative and qualitative data, formulate hypotheses on why drop-offs occur at certain stages.
-- MAGIC
-- MAGIC ##### Step 7: Testing & Validation
-- MAGIC * A/B Testing: If possible, perform A/B testing to validate the hypotheses.
-- MAGIC * Predictive Modeling: Use statistical models to predict drop-offs and validate findings against actual data.
-- MAGIC
-- MAGIC ##### Step 8: Actionable Insights
-- MAGIC * Insight Reporting: Compile a report with actionable insights and recommendations.
-- MAGIC * Strategic Recommendations: Propose specific changes or strategies to reduce drop-offs, such as improving the user experience, adjusting pricing, or enhancing follow-up communications.
-- MAGIC
-- MAGIC ##### Step 9: Monitoring
-- MAGIC * Implement Changes: Implement the proposed changes based on the insights.
-- MAGIC * Continuous Monitoring: Keep monitoring the conversion rates and customer feedback to assess the impact of the changes.
-- MAGIC
-- MAGIC ##### Step 10: Iterative Improvement
-- MAGIC * Iterative Analysis: Regularly revisit the analysis to find new insights and continuously improve the conversion rates.
-- MAGIC
