--Update Time: 4/7
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.project_usage` AS 
WITH license_user AS(
    SELECT compliance_key,machineid,grant_time,expire_time,license_type,institution,country_code_most_freq
    FROM `unity-other-learn-prd.reynafeng.academiclicense`
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT compliance_key,machineid,grant_time,expire_time,license_type,institution,country_code_most_freq
    FROM `unity-other-learn-prd.reynafeng.gamejam_license`
    GROUP BY 1,2,3,4,5,6,7
),

project_usage AS(
--Editor Package Manage--
SELECT raw.machine_id AS machineid, 
       raw.process_date AS submit_date,'Editor Create Project' AS editor_type,
       raw.cloud_project_id,
       CAST(raw.session_count AS INT64) AS session_count,
       build_size_byte AS build_size,
       SUM(user_duration_sec) AS action_time
FROM `unity-ai-unity-insights-prd.ai_editor_aggregate.project` raw
WHERE machine_id IN (SELECT DISTINCT machineid FROM license_user)
GROUP BY 1,2,3,4,5,6
)

SELECT A.*,D.submit_date AS project_date,D.editor_type,D.session_count,D.build_size,D.action_time,D.cloud_project_id
FROM license_user AS A
LEFT JOIN project_usage D ON D.machineid=A.machineid AND D.submit_date BETWEEN A.grant_time AND A.expire_time
