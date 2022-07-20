--Update Time: 7/11
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.license_region` AS

WITH user AS (
  SELECT compliance_key,machineid,license_hash,country_code_most_freq
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` A
  WHERE compliance_key IN (SELECT DISTINCT user_id FROM `unity-other-learn-prd.reynafeng.egl_grant_license`)
        OR
        compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.student_activation`)
        OR
        compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.educator_activation`)
  GROUP BY 1,2,3,4
),

region AS (
SELECT A.compliance_key,cloud_user_id,A.machineid,region,country_code,city
FROM(
SELECT compliance_key,head.cloud_user_id,head.license_hash,head.machineid,enrichments.geo_ip.region,enrichments.geo_ip.country_code,enrichments.geo_ip.city
FROM `unity-ai-data-prd.hub_general.hub_general_start_v1`
WHERE submit_date IS NOT NULL AND (compliance_key IN (SELECT DISTINCT compliance_key FROM user) OR head.machineid IN (SELECT machineid FROM user))
GROUP BY 1,2,3,4,5,6,7
UNION ALL
SELECT compliance_key,head.cloud_user_id,head.license_hash,head.machineid,enrichments.geo_ip.region,enrichments.geo_ip.country_code,enrichments.geo_ip.city
FROM `unity-ai-data-prd.hub_install.hub_install_editorDownloadStart_v1`
WHERE submit_date IS NOT NULL AND (compliance_key IN (SELECT DISTINCT compliance_key FROM user) OR head.machineid IN (SELECT machineid FROM user))
GROUP BY 1,2,3,4,5,6,7
UNION ALL
SELECT compliance_key,head.cloud_user_id,head.license_hash,head.machineid,enrichments.geo_ip.region,enrichments.geo_ip.country_code,enrichments.geo_ip.city
FROM `unity-ai-data-prd.hub_general.hub_general_licenseActivate_v1`
WHERE submit_date IS NOT NULL AND (compliance_key IN (SELECT DISTINCT compliance_key FROM user) OR head.machineid IN (SELECT machineid FROM user))
GROUP BY 1,2,3,4,5,6,7) A
JOIN user B ON A.compliance_key=B.compliance_key OR A.machineid=B.machineid
GROUP BY 1,2,3,4,5,6
)


SELECT A.compliance_key,
       cloud_user_id AS user_id,A.machineid,
       COALESCE(B.fullName,C.C_FirstAndLastName) AS fullName,
       COALESCE(B.email,C.C_EmailAddress,C.C_Email_Address_Clearbit1	) AS email,
       COALESCE(A.country_code,C.C_Country) AS country,
       COALESCE(A.region,C.C_State_Prov) AS state,
       COALESCE(A.city,C.C_City) AS city,C.C_Region11 AS region
FROM region A
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` B ON TO_BASE64(SHA256(CAST(B.id AS STRING)))=A.compliance_key
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights.marketing_eloqua_raw_restricted_contacts` C ON C.C_Compliance_Key1=A.compliance_key
WHERE A.compliance_key IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY 1,2
