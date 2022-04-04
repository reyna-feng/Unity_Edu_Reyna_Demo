--Update Time: 3/30
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.engagement` AS

SELECT A.*,B.* EXCEPT (machineid,sessionid,compliance_key,submit_date),B.submit_date AS editor_date,
       C.* EXCEPT (session_id,compliance_key,submit_date), C.submit_date AS learn_date,
       D.submit_date AS usage_date,D.action_type AS usage_type,D.num_action AS num_usage
FROM `unity-other-learn-prd.reynafeng.academiclicense` A
LEFT JOIN `unity-other-learn-prd.reynafeng.editor_usage` B ON B.sessionid=A.sessionid AND B.machineid=A.machineid AND B.compliance_key=A.compliance_key AND B.submit_date BETWEEN A.grant_time AND A.expire_time
LEFT JOIN `unity-other-learn-prd.reynafeng.learn` C ON C.session_id=CAST(A.sessionid AS STRING) AND C.compliance_key=A.compliance_key AND C.submit_date BETWEEN A.grant_time AND A.expire_time
LEFT JOIN `unity-other-learn-prd.reynafeng.other_usage` D ON C.compliance_key=A.compliance_key AND D.submit_date BETWEEN A.grant_time AND A.expire_time
