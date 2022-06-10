--Update Time: 6/7
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.editor_profile` AS
SELECT compliance_key,
       submit_date,
       CASE WHEN head.platform='' OR head.platform IS NULL THEN CAST(head.platformid AS STRING) ELSE head.platform END AS action_platform,
       CASE WHEN D.countryOfResidence LIKE '%=%' THEN B.C_Country ELSE D.countryOfResidence END AS country,
       CASE WHEN D.countryOfResidence LIKE '%=%' OR D.gender='' THEN NULL ELSE D.gender END AS gender,
       CASE WHEN D.countryOfResidence LIKE '%=%' OR D.preferredLocale='' OR D.preferredLocale IS NULL THEN C_Preferred_Language1 
            ELSE SPLIT(D.preferredLocale,'-')[safe_ordinal(2)] END AS locale,
       C_Industry1,C_Interest_in_Unity1,C_Department1,
       SPLIT(C_Region11,' - ')[safe_ordinal(1)] AS Region,
       C_Region11 AS Sub_Region,
       C_Company_Category_Industry_Clearbit1,
       C_Job_Primary_Role1
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_appStart_v1` raw
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` D ON TO_BASE64(SHA256(CAST(D.id AS STRING)))=raw.compliance_key
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights.marketing_eloqua_raw_restricted_contacts` B ON B.C_Compliance_Key1=raw.compliance_key
WHERE raw.submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13