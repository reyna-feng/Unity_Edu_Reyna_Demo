--KPI Report--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.academic_kpi` AS 

WITH startm AS(
SELECT start_month,COUNT(DISTINCT license_record_id) AS num_start,COUNT(DISTINCT institutionName) AS num_institution_start
FROM(
SELECT license_record_id ,license ,institutionName,
       MIN(DATE_TRUNC(request_time, month)) OVER(PARTITION BY license_record_id) AS start_month,
       MAX(DATE_TRUNC(expire_time, month)) OVER(PARTITION BY license_record_id) AS end_month
FROM `unity-other-learn-prd.reynafeng.egl_grant_license`
WHERE status='Approved') AS A
GROUP BY 1
ORDER BY 1
),

endm AS(
SELECT end_month,COUNT(DISTINCT license_record_id) AS num_end,COUNT(DISTINCT institutionName) AS num_institution_end
FROM(
SELECT license_record_id ,license ,institutionName,
       MIN(DATE_TRUNC(request_time, month)) OVER(PARTITION BY license_record_id) AS start_month,
       MAX(DATE_TRUNC(expire_time, month)) OVER(PARTITION BY license_record_id) AS end_month
FROM `unity-other-learn-prd.reynafeng.egl_grant_license`
WHERE status='Approved') AS A
GROUP BY 1
ORDER BY 1
),

institution AS(
SELECT *,
       AVG(running_balance) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_average,
       AVG(running_institution_balance) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_institution_average,
       
       SUM(running_balance) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_sum,
       SUM(running_institution_balance) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_institution_sum
FROM(
SELECT *,
       ROW_NUMBER() OVER(ORDER BY report_month) AS rnk,
       SUM(num_start) OVER(ORDER BY report_month) - SUM(num_end) OVER(ORDER BY report_month) AS running_balance,
       SUM(num_institution_start) OVER(ORDER BY report_month) - SUM(num_institution_end) OVER(ORDER BY report_month) AS running_institution_balance
FROM(       
SELECT IF(NOT start_month IS NULL, start_month, end_month) AS report_month,
       IF(NOT num_start IS NULL, num_start,0) AS num_start,
       IF(NOT num_end IS NULL, num_end,0) AS num_end,
       IF(NOT num_institution_start IS NULL, num_institution_start,0) AS num_institution_start,
       IF(NOT num_institution_end IS NULL, num_institution_end,0) AS num_institution_end
FROM startm
FULL JOIN endm ON startm.start_month = endm.end_month
) AS A
) AS B
),

monthly_student AS(
SELECT *,
       AVG(monthly_users) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_average,
       AVG(monthly_institution) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_institution_average,
       SUM(monthly_users) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_sum,
       SUM(monthly_institution) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_institution_sum
FROM(
SELECT visit_month,monthly_users,monthly_institution,
       ROW_NUMBER() OVER(ORDER BY visit_month) AS rnk
FROM `unity-other-learn-prd.reynafeng.egl_mau`
GROUP BY 1,2,3
ORDER BY 1) AS A
)


SELECT A.visit_month ,A.monthly_users ,A.monthly_institution ,
       IF(visit_month = DATE_TRUNC(CURRENT_DATE(),month),true,false) AS current_month,
       A.rolling_average AS monthly_rolling_students,A.rolling_institution_average AS monthly_rolling_schools,
       
       A.rolling_sum AS monthly_rolling_students_sum,A.rolling_institution_sum AS monthly_rolling_schools_sum,
       IF(NOT num_start IS NULL, num_start, 0) AS egl_license_start,
       IF(NOT num_end IS NULL, num_end, 0) AS egl_license_end,
       IF(NOT num_institution_start IS NULL, num_institution_start, 0) AS egl_school_start,
       IF(NOT num_institution_end IS NULL, num_institution_end, 0) AS egl_school_end,
       IF(NOT running_balance IS NULL, running_balance,0) AS egl_license_balance,
       IF(NOT running_institution_balance IS NULL, running_institution_balance,0) AS egl_school_balance,
       IF(NOT B.rolling_average IS NULL, B.rolling_average,0) AS monthly_rolling_egl_license,
       IF(NOT B.rolling_institution_average IS NULL, B.rolling_institution_average,0) AS monthly_rolling_egl_school,
       IF(NOT B.rolling_sum IS NULL, B.rolling_sum,0) AS monthly_rolling_egl_license_sum,
       IF(NOT B.rolling_institution_sum IS NULL, B.rolling_institution_sum,0) AS monthly_rolling_egl_school_sum,
       3 AS activation_multiplier
FROM monthly_student AS A
LEFT JOIN institution AS B ON A.visit_month = B.report_month

