--Update Time: 3/22--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.learn` AS

SELECT compliance_key,submit_date,head.user_id,head.logged_in,head.session_id,body.content_id,body.content_title,body.content_type,
       body.project_id,body.course_id,body.course_title,
       IF(head.user_premium=True, "Premium", "Free") AS premium,
       'Learn Start' AS learn_type
FROM `unity-ai-data-prd.learn_learner.learn_learner_itemStart_v1` item_start
WHERE submit_date IS NOT NULL
UNION ALL
SELECT compliance_key,submit_date,head.user_id,head.logged_in,head.session_id,body.content_id,body.content_title,body.content_type,
       body.project_id,body.course_id,body.course_title,
       IF(head.user_premium=True, "Premium", "Free") AS premium,
       'Learn Complete' AS learn_type
FROM `unity-ai-data-prd.learn_learner.learn_learner_itemComplete_v1` item_complete
WHERE submit_date IS NOT NULL
UNION ALL
SELECT compliance_key,submit_date,head.user_id,head.logged_in,head.session_id,body.content_id,body.content_title,body.content_type,
       body.project_id,body.course_id,body.course_title,
       IF(head.user_premium=True, "Premium", "Free") AS premium,
       'Learn Progress' AS learn_type
FROM `unity-ai-data-prd.learn_learner.learn_learner_stepUpdate_v1` item_update
WHERE submit_date IS NOT NULL
UNION ALL
SELECT compliance_key,submit_date,head.user_id,head.logged_in,head.session_id,
       body.content_id,body.content_title,body.content_type,
       body.project_id,body.course_id,body.course_title,
       IF(head.user_premium=True, "Premium", "Free") AS premium,
       'Learn Content Download' AS learn_type
FROM `unity-ai-data-prd.learn_learner.learn_learner_fileDownload_v1` content_download
WHERE submit_date IS NOT NULL