--Update Time: 3/14--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_flow_aggregation` AS

WITH steps AS(
SELECT DATE(grant_time) AS day_date,
       '1. Granted EGL License' AS step,
       SUM(IF(grant_time IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.egl_flow`
GROUP BY 1,2

UNION ALL

SELECT DATE(grant_time) AS day_date,
       '2. Activate License' AS step,
       SUM(IF(first_activation_ts IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.egl_flow`
GROUP BY 1,2

UNION ALL

SELECT DATE(grant_time) AS day_date,
       '3. Launch Hub' AS step,
       SUM(IF(first_hub_login IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.egl_flow`
GROUP BY 1,2

UNION ALL

SELECT DATE(grant_time) AS day_date,
       '4. Launch Editor' AS step,
       SUM(IF(first_editor_login IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.egl_flow`
GROUP BY 1,2

UNION ALL

SELECT DATE(grant_time) AS day_date,
       '5. Choose Microgame' AS step,
       SUM(IF(template_chosen IS NOT NULL, 1, 0)) AS user_count
FROM `unity-other-learn-prd.reynafeng.egl_flow`
GROUP BY 1,2
)

SELECT *,
       IF(NOT LAG(user_count_total) OVER(PARTITION BY day_date ORDER BY A.step) IS NULL,LAG(user_count_total) OVER(PARTITION BY day_date ORDER BY A.step),0) AS user_count_prev_step_total
FROM(
SELECT day_date,step,SUM(user_count) AS user_count_total
FROM steps
GROUP BY 1,2) AS A
ORDER BY 1 DESC,2 ASC
