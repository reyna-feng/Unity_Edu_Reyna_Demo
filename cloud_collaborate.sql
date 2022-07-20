--Update Time: 7/12
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.cloud_collaborate` AS

SELECT A.compliance_key,
       COALESCE(head.appid,body.project_id) AS project_id,
       head.environment,
       COALESCE(head.organizationid,body.org_id) AS org_id,
       COALESCE(head.sdk_ver,body.client_version) AS client_version,
       COALESCE(head.userid,body.user_id) AS user_id,
       body.ts,
       B.* EXCEPT(compliance_key,machineid,sessionid,if_expired,user_id),
       'Create Porject' AS action,
       CAST(NULL AS STRING) AS branch,NULL AS commit_size,CAST(NULL AS STRING) AS build_status
FROM `unity-ai-data-prd.cloud_collaborate_raw.cloud_collaborate_createProject_v1` A
JOIN `unity-other-learn-prd.reynafeng.academiclicense` B ON B.user_id = COALESCE(A.head.userid,A.body.user_id)
      AND DATE(body.ts) BETWEEN grant_time AND expire_time
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

UNION ALL

SELECT A.compliance_key,
       COALESCE(head.appid,body.project_id) AS project_id,
       head.environment,
       COALESCE(head.organizationid,body.org_id) AS org_id,
       COALESCE(head.sdk_ver,body.client_version) AS client_version,
       COALESCE(head.userid,body.user_id) AS user_id,
       body.ts,
       B.* EXCEPT(compliance_key,machineid,sessionid,if_expired,user_id),
       'Publish Failure' AS action,
       body.branch,body.commit_size,CAST(NULL AS STRING) AS build_status
FROM `unity-ai-data-prd.cloud_collaborate_raw.cloud_collaborate_publishFailure_v1` A
JOIN `unity-other-learn-prd.reynafeng.academiclicense` B ON B.user_id = COALESCE(A.head.userid,A.body.user_id)
      AND DATE(body.ts) BETWEEN grant_time AND expire_time
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22

UNION ALL

SELECT A.compliance_key,
       COALESCE(head.appid,body.project_id) AS project_id,
       head.environment,
       COALESCE(head.organizationid,body.org_id) AS org_id,
       COALESCE(head.sdk_ver,body.client_version) AS client_version,
       COALESCE(head.userid,body.user_id) AS user_id,
       body.ts,
       B.* EXCEPT(compliance_key,machineid,sessionid,if_expired,user_id),
       'Publish' AS action,
       body.branch,body.commit_size,CAST(NULL AS STRING) AS build_status
FROM `unity-ai-data-prd.cloud_collaborate_raw.cloud_collaborate_publish_v1` A
JOIN `unity-other-learn-prd.reynafeng.academiclicense` B ON B.user_id = COALESCE(A.head.userid,A.body.user_id)
      AND DATE(body.ts) BETWEEN grant_time AND expire_time
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22

UNION ALL

SELECT A.compliance_key,
       COALESCE(head.appid,body.project_id) AS project_id,
       head.environment,
       COALESCE(head.organizationid,body.org_id) AS org_id,
       COALESCE(head.sdk_ver,body.client_version) AS client_version,
       COALESCE(head.userid,body.user_id) AS user_id,
       body.ts,
       B.* EXCEPT(compliance_key,machineid,sessionid,if_expired,user_id),
       'Update Build Status' AS action,
       CAST(NULL AS STRING) AS branch,NULL AS commit_size,body.build_status
FROM `unity-ai-data-prd.cloud_collaborate_raw.cloud_collaborate_updateBuildState_v1` A
JOIN `unity-other-learn-prd.reynafeng.academiclicense` B ON B.user_id = COALESCE(A.head.userid,A.body.user_id)
      AND DATE(body.ts) BETWEEN grant_time AND expire_time
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
