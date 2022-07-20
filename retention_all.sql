--Update Time: 7/12
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.retention_all` AS

WITH user AS (
  SELECT A.compliance_key,A.machineid,A.license_hash,country_code_most_freq,DATE_TRUNC(first_login_date,MONTH) AS cohort_month
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` A
  GROUP BY 1,2,3,4,5
),

asset_store AS (
SELECT *,
       MIN(as_date) OVER(PARTITION BY compliance_key ORDER BY as_date) AS as_conversion
FROM(
SELECT DATE_TRUNC(A.submit_date,MONTH) AS as_date,A.compliance_key,SUM(body.amount_final_usd) AS as_revenue
FROM `unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packagePurchased_v2` A
WHERE submit_date IS NOT NULL AND body.amount_final_usd!=0
GROUP BY 1,2) A
),

retention AS(
SELECT B.*,
       IF(NOT C.as_revenue IS NULL, C.as_revenue,0.0) AS as_revenue,
       CASE WHEN B.hub_login=C.as_conversion THEN 1 ELSE 0 END AS as_conversion,
       DATE_DIFF(hub_login,cohort_month, month) AS month_aged
FROM(
SELECT A.compliance_key,A.hub_login,
       D.Email,
       COALESCE(country_code_most_freq,D.countryOfResidence) AS country,
       MIN(hub_login) OVER(PARTITION BY compliance_key) AS cohort_month
FROM(
    SELECT COALESCE(mapping.compliance_key,user.compliance_key) AS compliance_key,
           DATE_TRUNC(mapping.submit_date,MONTH) AS hub_login,user.country_code_most_freq,user.cohort_month
    FROM(
      SELECT compliance_key,head.machineid,head.license_hash,submit_date
      FROM `unity-ai-data-prd.hub_general.hub_general_start_v1`
      WHERE submit_date IS NOT NULL) mapping
    FULL OUTER JOIN user ON mapping.machineid=user.machineid AND mapping.license_hash=user.license_hash
    WHERE mapping.submit_date IS NOT NULL
    GROUP BY 1,2,3,4) AS A
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` AS D ON TO_BASE64(SHA256(CAST(D.id AS STRING)))=A.compliance_key
GROUP BY 1,2,3,4) B
LEFT JOIN asset_store C ON B.compliance_key=C.compliance_key AND B.hub_login=C.as_date
)


SELECT cohort_month,month_aged,country,academic_users,num_user,as_revenue,
       num_user/first_num_user AS retention,
       running_as_conversion/first_num_user AS conversion_as,
       running_as_revenue/first_num_user AS arpu_as
FROM(
SELECT *,
       FIRST_VALUE(num_user) OVER (PARTITION BY cohort_month,academic_users,country ORDER BY month_aged ASC) AS first_num_user,
       SUM(as_conversion) OVER(PARTITION BY cohort_month,academic_users,country ORDER BY month_aged ASC) AS running_as_conversion,
       SUM(as_revenue) OVER(PARTITION BY cohort_month,academic_users,country ORDER BY month_aged ASC) AS running_as_revenue
FROM(
SELECT cohort_month,
       month_aged,country,
       CASE WHEN B.compliance_key IS NOT NULL THEN 'Edu' ELSE 'Non Edu' END AS academic_users,
       SUM(as_revenue) AS as_revenue, SUM(as_conversion) AS as_conversion,
       COUNT(DISTINCT A.compliance_key) AS num_user
FROM retention A
LEFT JOIN (
  SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`
) B ON A.compliance_key=B.compliance_key
GROUP BY 1,2,3,4
ORDER BY 1,2) AS A
ORDER BY 1,2) AS B
