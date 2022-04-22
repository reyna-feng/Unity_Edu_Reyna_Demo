--Update Time 4/4

CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.academic_kpi` AS
SELECT *,
       student_sp+student_egl+student_edlab AS total_students,
       school_egl+school_edlab+paying_school AS total_school,
       educator_ep+educator_community+school_egl+school_edlab+paying_school AS total_educator
FROM(
SELECT A.visit_month,A.current_month,A.monthly_rolling_seats_sum AS student_sp,
       A.MAU AS student_mau,
       IF(NOT B.grantCount IS NULL, B.grantCount, 0) AS egl_granted,
       IF(NOT B.monthly_users IS NULL, B.monthly_users, 0) AS egl_mau,
       IF(NOT B.monthly_rolling_seats IS NULL, B.monthly_users, 0) AS egl_seats_mau,
       IF(NOT C.MAU IS NULL, C.MAU, 0) AS educator_mau,
       IF(NOT B.egl_license_balance IS NULL,B.egl_school_balance,0) AS school_egl,
       3*IF(NOT B.monthly_rolling_seats_sum IS NULL,B.monthly_rolling_seats_sum,0) AS student_egl,
       0 AS student_edlab,
       0 AS school_edlab,
       0 AS paying_school,
       IF(NOT C.monthly_rolling_seats_sum IS NULL, C.monthly_rolling_seats_sum,0) AS educator_ep,
       0 AS educator_community
FROM `unity-other-learn-prd.reynafeng.sp_academic_kpi` A
LEFT JOIN `unity-other-learn-prd.reynafeng.egl_academic_kpi` B ON A.visit_month=B.visit_month 
LEFT JOIN `unity-other-learn-prd.reynafeng.ep_academic_kpi` C ON A.visit_month=C.visit_month 
WHERE A.visit_month>='2020-02-01'
ORDER BY 1 ASC) AS A
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18