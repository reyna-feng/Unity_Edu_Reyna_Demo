CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.student_flow_aggregation` AS

WITH steps AS(

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '1. Granted Student License' AS step,
       SUM(IF(licnese_grant_time IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '2. Activate License' AS step,
       SUM(IF(first_activation_ts IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '3. Launch Hub' AS step,
       SUM(IF(first_hub_login IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '4. Launch Editor' AS step,
       SUM(IF(first_editor_login IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '5. Choose Microgame' AS step,
       SUM(IF(template_chosen IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '6. Complete all WTs' AS step,
       SUM(IF(wt_completed_count >= 5, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE(licnese_grant_time) AS day_date,account_type,license,
       '7. Share WebGL Game' AS step,
       SUM(IF(webgl_post_first_ts IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.student_flow`
GROUP BY 1,2,3,4

)

SELECT *,
       LAG(user_count_total) OVER(PARTITION BY day_date ORDER BY A.step) AS user_count_prev_step_total
FROM(
SELECT day_date,step,SUM(user_count) AS user_count_total
FROM steps
GROUP BY 1,2) AS A
