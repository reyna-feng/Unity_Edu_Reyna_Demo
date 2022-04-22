--Update Time: 4/6
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.other_usage` AS 

WITH license_user AS(
    SELECT compliance_key,machineid,grant_time,expire_time,license_type,institution,country_code_most_freq
    FROM `unity-other-learn-prd.reynafeng.academiclicense`
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT compliance_key,machineid,grant_time,expire_time,license_type,institution,country_code_most_freq
    FROM `unity-other-learn-prd.reynafeng.gamejam_license`
    GROUP BY 1,2,3,4,5,6,7
),
usage AS(
--Web Portabl Login--
SELECT compliance_key,submit_date,'Portal Login' AS action_type,
       COUNT(*) AS num_action
FROM `unity-ai-data-prd.udp_portal_raw.udp_portal_UserLogin_v1` raw
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM license_user)
GROUP BY 1,2,3

--Collab Usage--

--Webinars--
)


SELECT A.*,D.submit_date AS usage_date,D.action_type,D.num_action
FROM license_user AS A
LEFT JOIN usage D ON D.compliance_key=A.compliance_key AND D.submit_date BETWEEN A.grant_time AND A.expire_time
