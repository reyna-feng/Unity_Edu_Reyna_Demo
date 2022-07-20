--Update Time: 7/12
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.retention` AS

WITH user AS (
  SELECT A.compliance_key,A.machineid,A.license_hash,country_code_most_freq
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` A
  WHERE compliance_key IN (SELECT DISTINCT user_id FROM `unity-other-learn-prd.reynafeng.egl_grant_license`)
        OR
        compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.student_activation`)
        OR
        compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.educator_activation`)
  GROUP BY 1,2,3,4
),

asset_store AS (
SELECT *,
       MIN(as_date) OVER(PARTITION BY compliance_key ORDER BY as_date) AS as_conversion
FROM(
SELECT DATE_TRUNC(A.submit_date,MONTH) AS as_date,A.compliance_key,SUM(amount_final_usd) AS as_revenue
FROM `unity-other-learn-prd.reynafeng.asset_store` A
LEFT JOIN `unity-other-learn-prd.reynafeng.student_activation` B ON A.compliance_key=B.compliance_key AND A.submit_date>=DATE(B.licnese_grant_time)
LEFT JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` C ON A.compliance_key=C.user_id AND A.submit_date>=C.grant_time
LEFT JOIN `unity-other-learn-prd.reynafeng.educator_activation` D ON A.compliance_key=D.compliance_key AND A.submit_date>=DATE(D.licnese_grant_time)
GROUP BY 1,2) A
),

retention AS(
SELECT *,
       DATE_DIFF(hub_login,first_hub_login, month) AS month_aged
FROM(
SELECT B.*,
       IF(NOT C.as_revenue IS NULL, C.as_revenue,0.0) AS as_revenue,
       CASE WHEN B.hub_login=C.as_conversion THEN 1 ELSE 0 END AS as_conversion,
       MIN(hub_login) OVER(PARTITION BY B.compliance_key) AS first_hub_login
FROM(
SELECT A.compliance_key,
       DATE_TRUNC(A.submit_date, month) AS hub_login,
       D.Email,D.countryOfResidence,country_code_most_freq,SPLIT(D.Email,'@')[safe_ordinal(2)] AS domain,
       IF(NOT B.compliance_key IS NULL, True, False) AS sp,
       IF(NOT C.user_id IS NULL, True, False) AS egl,
       IF(NOT E.compliance_key IS NULL, True, False) AS ep,
FROM(
    SELECT COALESCE(mapping.compliance_key,user.compliance_key) AS compliance_key,
           mapping.submit_date,user.country_code_most_freq
    FROM(
      SELECT compliance_key,head.machineid,head.license_hash,submit_date
      FROM `unity-ai-data-prd.hub_general.hub_general_start_v1`
      WHERE submit_date IS NOT NULL) mapping
    FULL OUTER JOIN user ON mapping.machineid=user.machineid AND mapping.license_hash=user.license_hash
    WHERE mapping.submit_date IS NOT NULL
    GROUP BY 1,2,3) AS A
LEFT JOIN `unity-other-learn-prd.reynafeng.student_activation` B ON A.compliance_key=B.compliance_key AND A.submit_date>=DATE(B.licnese_grant_time)
LEFT JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` C ON A.compliance_key=C.user_id AND A.submit_date>=C.grant_time
LEFT JOIN `unity-other-learn-prd.reynafeng.educator_activation` E ON A.compliance_key=E.compliance_key AND A.submit_date>=DATE(E.licnese_grant_time)
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` AS D ON TO_BASE64(SHA256(CAST(D.id AS STRING)))=A.compliance_key
WHERE B.compliance_key IS NOT NULL OR C.user_id IS NOT NULL OR E.compliance_key IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9
) AS B
LEFT JOIN asset_store C ON B.compliance_key=C.compliance_key AND B.hub_login=C.as_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11) AS C
)

SELECT cohort_month,month_aged,country_code_most_freq,domain,license,num_user,as_revenue,
       num_user/first_num_user AS retention,
       running_as_conversion/first_num_user AS conversion_as,
       running_as_revenue/first_num_user AS arpu_as
FROM(
SELECT *,
       FIRST_VALUE(num_user) OVER (PARTITION BY cohort_month,license,country_code_most_freq,domain ORDER BY month_aged ASC) AS first_num_user,
       SUM(as_conversion) OVER(PARTITION BY cohort_month,license,country_code_most_freq,domain ORDER BY month_aged ASC) AS running_as_conversion,
       SUM(as_revenue) OVER(PARTITION BY cohort_month,license,country_code_most_freq,domain ORDER BY month_aged ASC) AS running_as_revenue
FROM(
SELECT first_hub_login AS cohort_month,
       month_aged,country_code_most_freq,domain,
       CASE WHEN sp=True THEN 'Student Plan'
            WHEN egl=True THEN 'EGL'
            WHEN ep=True THEN 'Educator Plan' END AS license,
       SUM(as_revenue) AS as_revenue, SUM(as_conversion) AS as_conversion,
       COUNT(DISTINCT compliance_key) AS num_user
FROM retention
GROUP BY 1,2,3,4,5
ORDER BY 1,2) AS A
ORDER BY 1,2) AS B
