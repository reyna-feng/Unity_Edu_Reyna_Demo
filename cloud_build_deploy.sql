--Update Time: 7/12
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.cloud_build_deploy` AS

SELECT A.compliance_key,body.build_attempt_number,body.build_target_platform,
       body.deploy_status,body.deploy_target,body.ts,
       body.org_id,body.user_id,body.appid,body.build_target_name,
       B.* EXCEPT(compliance_key,machineid,sessionid,if_expired,user_id)
FROM `unity-ai-data-prd.cloud_build_raw.cloud_build_buildDeploy_v1` A
JOIN `unity-other-learn-prd.reynafeng.academiclicense` B ON B.user_id = A.body.user_id
      AND DATE(body.ts) BETWEEN grant_time AND expire_time
WHERE submit_date IS NOT NULL 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21