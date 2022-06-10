--Update time: 6/8
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.egl_renewed_license` AS

WITH renew AS (
SELECT DATE_TRUNC(grant_time,MONTH) AS renew_month,COUNT(DISTINCT license) AS num_renew
FROM `unity-other-learn-prd.reynafeng.egl_grant_license`
WHERE is_renew=True
GROUP BY 1
),

expire AS (
SELECT DATE_TRUNC(end_month,MONTH) AS end_month,COUNT(DISTINCT license) AS num_expire
FROM `unity-other-learn-prd.reynafeng.egl_expired_license`
GROUP BY 1
)

SELECT end_month,num_expire,IF(NOT num_renew IS NULL, num_renew,0) AS num_renew,
       SUM(IF(NOT num_renew IS NULL, num_renew,0)) OVER(ORDER BY end_month) / SUM(num_expire) OVER(ORDER BY end_month) AS renewal_rate
FROM expire
LEFT JOIN renew ON end_month=renew_month
ORDER BY end_month