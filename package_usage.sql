--Update Time: 4/7
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.package_usage` AS 
WITH license_user AS(
    SELECT *
    FROM `unity-other-learn-prd.reynafeng.academiclicense`
    UNION ALL
    SELECT *
    FROM `unity-other-learn-prd.reynafeng.gamejam_license`
),
package AS(
--Editor Package Manage--
SELECT head.machineid, head.sessionid,
       compliance_key,submit_date,'Editor Package Manage' AS editor_type,
       CASE WHEN head.platform='' OR head.platform IS NULL THEN CAST(head.platformid AS STRING) ELSE head.platform END AS action_platform,
       head.session_count,
       body.package_id AS package_id,
       0 AS build_size,
       0 AS action_time,
       body.action AS action_type
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_packageManagerWindowUserAction_v1` raw
WHERE submit_date IS NOT NULL AND head.sessionid IN (SELECT DISTINCT sessionid FROM license_user)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11

UNION ALL

SELECT head.machineid, head.sessionid,
       raw.compliance_key,raw.submit_date,'Editor Add Package' AS editor_type,
       CASE WHEN head.platform='' OR head.platform IS NULL THEN CAST(head.platformid AS STRING) ELSE head.platform END AS action_platform,
       head.session_count,
       body.package_id AS package_id,
       0 AS build_size,
       SUM(body.duration/1000000) AS action_time,
       'Editor Add Package' AS action_type
FROM `unity-ai-data-prd.editor_packageManager.editor_packageManager_addPackage_v1` raw
WHERE submit_date IS NOT NULL AND head.sessionid IN (SELECT DISTINCT sessionid FROM license_user)
GROUP BY 1,2,3,4,5,6,7,8,9,11

UNION ALL
--Editor Tutorial--
SELECT head.machineid, head.sessionid,
       compliance_key,submit_date,'Editor Tutorial Page' AS editor_type,
       CASE WHEN head.platform='' OR head.platform IS NULL THEN CAST(head.platformid AS STRING) ELSE head.platform END AS action_platform,
       head.session_count,
       body.package AS package_id,
       0 AS build_size,
       SUM(body.duration) AS action_time,
       body.tutorialName AS action_type
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_iet_tutorialPage_v1` raw
WHERE submit_date IS NOT NULL AND head.sessionid IN (SELECT DISTINCT sessionid FROM license_user)
GROUP BY 1,2,3,4,5,6,7,8,9,11
)

SELECT A.*,B.* EXCEPT (machineid,sessionid,compliance_key,submit_date),B.submit_date AS editor_date
FROM license_user A
LEFT JOIN package B ON B.sessionid=A.sessionid AND B.machineid=A.machineid AND B.compliance_key=A.compliance_key AND B.submit_date BETWEEN A.grant_time AND A.expire_time
