--Update Time: 7/18
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.cloud_build_attempt` AS

SELECT body.build_target_platform,body.scm_type,body.artifact_size,body.build_status, body.ts,
       body.buildtime_seconds,body.checkout_start_time,body.checkout_time_seconds,body.project_size,
       body.total_time_seconds,
       TO_BASE64(SHA256(CAST(A.body.org_id AS STRING))) AS compliance_key,
       A.body.user_id,A.body.workspace_size,A.body.appid,
       B.* EXCEPT(compliance_key,machineid,sessionid,if_expired,user_id)
FROM `unity-ai-data-prd.cloud_build_raw.cloud_build_buildAttempt_v1` A
JOIN `unity-other-learn-prd.reynafeng.academiclicense` B ON B.user_id = A.body.user_id
      AND DATE(body.ts) BETWEEN grant_time AND expire_time
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25