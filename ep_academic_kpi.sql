--Update Time: 7/17
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.ep_academic_kpi` AS 

WITH installs AS(
SELECT install_month,num_install,num_expire,
       SUM(num_install) OVER(ORDER BY install_month) - SUM(num_expire) OVER(ORDER BY install_month) AS running_installs
FROM(
SELECT DATE_TRUNC(install_time,month) AS install_month,
       SUM(installs) AS num_install,
       SUM(expires) AS num_expire
FROM(
  SELECT IF(NOT install_time IS NULL, install_time, expire_time) AS install_time,
         IF(NOT installs IS NULL, installs, 0) AS installs,
         IF(NOT expires IS NULL, expires, 0) AS expires
  FROM(
      SELECT DATE(first_activation_ts) AS install_time, COUNT(DISTINCT compliance_key) AS installs
      FROM `unity-other-learn-prd.reynafeng.educator_activation`
      WHERE activation_status=True AND DATE(first_activation_ts)>='2021-04-13'
      GROUP BY 1
  ) AS A
  FULL OUTER JOIN (
          SELECT DATE(licnese_expiration_time) AS expire_time, COUNT(DISTINCT compliance_key) AS expires
          FROM `unity-other-learn-prd.reynafeng.educator_activation`
          WHERE activation_status=True AND DATE(first_activation_ts)>='2021-04-13'
          GROUP BY 1
  ) AS B ON A.install_time=B.expire_time
) AS C
GROUP BY 1
) AS D
),

startm AS(
SELECT start_month,COUNT(DISTINCT compliance_key) AS num_start
FROM(
SELECT compliance_key,serialNumber,
       MIN(DATE_TRUNC(DATE(licnese_grant_time), month)) OVER(PARTITION BY compliance_key,serialNumber) AS start_month,
       MAX(DATE_TRUNC(DATE(licnese_expiration_time), month)) OVER(PARTITION BY compliance_key,serialNumber) AS end_month
FROM `unity-other-learn-prd.reynafeng.educator_activation`
--WHERE activation_status=True
) AS A
GROUP BY 1
ORDER BY 1
),

endm AS(
SELECT end_month,COUNT(DISTINCT compliance_key) AS num_end
FROM(
SELECT compliance_key,serialNumber,
       MIN(DATE_TRUNC(DATE(licnese_grant_time), month)) OVER(PARTITION BY compliance_key,serialNumber) AS start_month,
       MAX(DATE_TRUNC(DATE(licnese_expiration_time), month)) OVER(PARTITION BY compliance_key,serialNumber) AS end_month
FROM `unity-other-learn-prd.reynafeng.educator_activation`
--WHERE activation_status=True
) AS A
GROUP BY 1
ORDER BY 1
),

institution AS(
SELECT *,
       ROW_NUMBER() OVER(ORDER BY report_month) AS rnk,
       SUM(num_start) OVER(ORDER BY report_month) - SUM(num_end) OVER(ORDER BY report_month) AS running_balance
FROM(       
SELECT DATE_TRUNC(COALESCE(start_month,end_month,install_month),month) AS report_month,
       IF(NOT num_start IS NULL, num_start,0) AS num_start,
       IF(NOT num_end IS NULL, num_end,0) AS num_end,
       IF(NOT installs.num_install IS NULL, installs.num_install,0) AS num_install,
       IF(NOT installs.num_expire IS NULL, installs.num_expire,0) AS num_expire,
       IF(NOT installs.running_installs IS NULL, installs.running_installs,0) AS running_installs
FROM startm
FULL OUTER JOIN endm ON startm.start_month = endm.end_month
FULL OUTER JOIN installs ON COALESCE(start_month,end_month) = installs.install_month
) AS A
ORDER BY report_month
),

monthly_student AS(
SELECT *,
       AVG(MAU) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_average,
       SUM(MAU) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_sum
FROM(
SELECT visit_month,MAU,
       ROW_NUMBER() OVER(ORDER BY visit_month) AS rnk
FROM `unity-other-learn-prd.reynafeng.educator_mau`
GROUP BY 1,2
ORDER BY 1) AS A
)

SELECT *,
       LAG(num_install) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) AS lag_num_install,
       LAG(educator_license_start) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) AS lag_educator_license_start,
       IF(educator_license_end=0, NULL, 1-educator_license_end/LAG(educator_license_start) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month))) AS renew_license
FROM(
SELECT A.visit_month ,A.MAU ,
       IF(visit_month = DATE_TRUNC(CURRENT_DATE(),month),true,false) AS current_month,
       A.rolling_average AS monthly_rolling_educators,
       
       A.rolling_sum AS monthly_rolling_educators_sum,
       IF(NOT num_start IS NULL, num_start, 0) AS educator_license_start,
       IF(NOT num_end IS NULL, num_end, 0) AS educator_license_end,
       IF(NOT running_balance IS NULL, running_balance,0) AS educator_license_balance,
       IF(NOT B.num_install IS NULL, B.num_install,0) AS num_install,
       IF(NOT B.num_expire IS NULL, B.num_expire,0) AS num_expire,
       IF(NOT B.running_installs IS NULL, B.running_installs,0) AS monthly_rolling_seats_sum
FROM monthly_student AS A
LEFT JOIN institution AS B ON A.visit_month = B.report_month) AS A