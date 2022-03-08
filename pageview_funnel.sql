CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.pageview_funnel` AS

SELECT submit_date,'Learn' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.domain='unity.com' AND head.path='/learn'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'EGL Product' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.path='/products/unity-education-grant-license'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'Student Plan Product' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.path='/products/unity-student'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'Educator Plan Product' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.path='/products/unity-educator'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'UAA Product' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.path='/products/unity-academic-alliance'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'EdLab Product' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.path='/products/unity-edlab'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'EGL Apply Now' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.domain='learn.unity.com' AND head.path='/education-grant-license'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'Student/Educator Plan Apply Now' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL 
      AND ((head.domain='id.unity.com' AND head.path LIKE '/conversations%') OR (head.domain='learn.unity.com' AND head.path='/education-grant-license?page=countrySelection'))
GROUP BY 1,2

UNION ALL

SELECT submit_date,'UAA Join Now' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.domain='create.unity.com' AND head.path='/academic-alliance-interest'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'EdLab Join Now' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.domain='create.unity.com' AND head.path='/unity-edlab-sales-rep'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'Student Plan Claim Licenses' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.path='/verification/student/success'
GROUP BY 1,2

UNION ALL

SELECT submit_date,'Educator Plan Claim Licenses' AS page,COUNT(*) AS users
FROM `unity-ai-data-prd.unity_web_raw.unity_web_pageView_v1`
WHERE submit_date IS NOT NULL AND head.path='/licenseApplied'
GROUP BY 1,2