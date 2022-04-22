--Update Time: 4/6--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.learn` AS

WITH license_user AS(
    SELECT compliance_key,machineid,grant_time,expire_time,license_type,institution,country_code_most_freq
    FROM `unity-other-learn-prd.reynafeng.academiclicense`
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT compliance_key,machineid,grant_time,expire_time,license_type,institution,country_code_most_freq
    FROM `unity-other-learn-prd.reynafeng.gamejam_license`
    GROUP BY 1,2,3,4,5,6,7
),
learn AS(
SELECT compliance_key,submit_date,head.user_id,head.logged_in,head.session_id,
       body.content_title,body.content_type,body.project_title,body.course_title,body.pathway_title,
       body.content_level,body.content_industry,0 AS step_number,
       IF(body.content_premium="true","Premium","Free") AS content_premium,
       IF(head.user_premium=True, "Premium", "Free") AS user_premium,
       'Learn Start' AS learn_type,
       0 AS final_score, false AS passed, NULL AS file_name, body.content_topic
FROM `unity-ai-data-prd.learn_learner.learn_learner_itemStart_v1` item_start
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM license_user)
      AND body.item_start = true

UNION ALL

SELECT compliance_key,submit_date,head.user_id,head.logged_in,head.session_id,
       body.content_title,body.content_type,body.project_title,body.course_title,body.pathway_title,
       body.content_level,body.content_industry,0 AS step_number,
       IF(body.content_premium="true","Premium","Free") AS content_premium,
       IF(head.user_premium=True, "Premium", "Free") AS premium,
       'Learn Complete' AS learn_type,
       CAST(IFNULL(body.final_score,'0') AS INT64) AS final_score,body.passed,NULL AS file_name,body.content_topic
FROM `unity-ai-data-prd.learn_learner.learn_learner_itemComplete_v1` item_complete
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM license_user)
      AND body.item_complete = true

UNION ALL

SELECT compliance_key,submit_date,head.user_id,head.logged_in,head.session_id,
       body.content_title,body.content_type,body.project_title,body.course_title,body.pathway_title,
       body.content_level,body.content_industry,body.step_number,
       IF(body.content_premium="true","Premium","Free") AS content_premium,
       IF(head.user_premium=True, "Premium", "Free") AS user_premium,
       'Learn Progress' AS learn_type,
       0 AS final_score, false AS passed,NULL AS file_name,body.content_topic
FROM `unity-ai-data-prd.learn_learner.learn_learner_stepUpdate_v1` item_update
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM license_user)
      AND body.step_complete = true
      
UNION ALL

SELECT compliance_key,submit_date,head.user_id,head.logged_in,head.session_id,
       body.content_title,body.content_type,body.project_title,body.course_title,NULL AS pathway_title,
       body.content_level,body.content_industry,0 AS step_number,
       IF(body.content_premium="true","Premium","Free") AS content_premium,
       IF(head.user_premium=True, "Premium", "Free") AS user_premium,
       'Learn Content Download' AS learn_type,
       0 AS final_score, false AS passed,body.file_name,body.content_topic
FROM `unity-ai-data-prd.learn_learner.learn_learner_fileDownload_v1` content_download
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM license_user)
)


SELECT A.*,
       C.* EXCEPT (session_id,compliance_key,submit_date), C.submit_date AS learn_date
FROM license_user A
LEFT JOIN learn C ON C.compliance_key=A.compliance_key AND C.submit_date BETWEEN A.grant_time AND A.expire_time