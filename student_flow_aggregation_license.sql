--Update Time: 3/10 9:18 PM--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.student_flow_aggregation_license` AS

WITH steps AS(
SELECT submit_date AS day_date,"Web Visitor" AS account_type,"Web Visitor" AS license,
       '1. View Learn Unity Page' AS step,
       SUM(users) AS user_count
FROM `unity-other-learn-prd.reynafeng.pageview_funnel`
WHERE page='Learn'
GROUP BY 1,2,3,4

UNION ALL

SELECT submit_date AS day_date,"Web Visitor" AS account_type,"Web Visitor" AS license,
       '2. EGL Apply Now Page' AS step,
       SUM(users) AS user_count
FROM `unity-other-learn-prd.reynafeng.pageview_funnel`
WHERE page='Student/Educator Plan Apply Now'
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '3. Granted Student License' AS step,
       SUM(IF(licnese_grant_time IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '4. Activate License' AS step,
       SUM(IF(first_activation_ts IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '5. Launch Hub' AS step,
       SUM(IF(first_hub_login IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '6. Launch Editor' AS step,
       SUM(IF(first_editor_login IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '7. Choose Microgame' AS step,
       SUM(IF(template_chosen IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4
)

SELECT *,
       LAG(user_count_total_license) OVER(PARTITION BY day_date,license ORDER BY A.step) AS user_count_prev_step_total_license
FROM(
SELECT day_date,license,step,SUM(user_count) AS user_count_total_license
FROM steps
GROUP BY 1,2,3) AS A