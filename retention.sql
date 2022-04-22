--Update Time: 4/12
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

retention AS(
SELECT *,
       DATE_DIFF(hub_login,first_hub_login, month) AS month_aged
FROM(
SELECT *,
       MIN(hub_login) OVER(PARTITION BY compliance_key) AS first_hub_login
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
LEFT JOIN `unity-other-learn-prd.reynafeng.student_activation` B ON A.compliance_key=B.compliance_key AND A.submit_date BETWEEN LEAST(DATE(B.licnese_create_time),DATE(B.licnese_grant_time)) AND DATE(B.licnese_expiration_time)
LEFT JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` C ON A.compliance_key=C.user_id AND A.submit_date BETWEEN C.grant_time AND C.expire_time
LEFT JOIN `unity-other-learn-prd.reynafeng.educator_activation` E ON A.compliance_key=E.compliance_key AND A.submit_date BETWEEN LEAST(DATE(E.licnese_create_time),DATE(E.licnese_grant_time)) AND DATE(E.licnese_expiration_time)

LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` AS D ON TO_BASE64(SHA256(CAST(D.id AS STRING)))=A.compliance_key
WHERE B.compliance_key IS NOT NULL OR C.user_id IS NOT NULL OR E.compliance_key IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9
) AS B
GROUP BY 1,2,3,4,5,6,7,8,9) AS C
)

SELECT *,
       num_user/first_num_user AS retention
FROM(
SELECT *,
       FIRST_VALUE(num_user) OVER (PARTITION BY cohort_month,license,country_code_most_freq,domain ORDER BY month_aged ASC) AS first_num_user
FROM(
SELECT first_hub_login AS cohort_month,
       month_aged,country_code_most_freq,domain,
       CASE WHEN sp=True THEN 'Student Plan'
            WHEN egl=True THEN 'EGL'
            WHEN ep=True THEN 'Educator Plan' END AS license,
       COUNT(DISTINCT compliance_key) AS num_user
FROM retention
GROUP BY 1,2,3,4,5
ORDER BY 1,2) AS A
ORDER BY 1,2) AS B