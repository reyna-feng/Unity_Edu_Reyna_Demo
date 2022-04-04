--Update Time: 3/29--
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.pageview_funnel` AS

SELECT day,'Learn' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE hostname='unity.com' AND full_page_path='/learn'
GROUP BY 1,2

UNION ALL
--Product Page--
SELECT day,'EGL Product' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE hostname='unity.com' AND full_page_path='/products/unity-education-grant-license'
GROUP BY 1,2

UNION ALL

SELECT day,'Student Plan Product' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE hostname='unity.com' AND full_page_path='/products/unity-student'
GROUP BY 1,2

UNION ALL

SELECT day,'Educator Plan Product' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE hostname='unity.com' AND full_page_path='/products/unity-educator'
GROUP BY 1,2

UNION ALL

SELECT day,'UAA Product' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE hostname='unity.com' AND full_page_path='/products/unity-academic-alliance'
GROUP BY 1,2

UNION ALL

SELECT day,'EdLab Product' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE hostname='unity.com' AND full_page_path='/products/unity-edlab'
GROUP BY 1,2

UNION ALL
--EGL: Select Country--
SELECT day,'EGL Select Country' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=countrySelection%'
GROUP BY 1,2

UNION ALL
--EGL: Select Institution--
SELECT day,'EGL Select Institution' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=institutionSelection%'
GROUP BY 1,2

UNION ALL
--EGL: Step 1--
SELECT day,'EGL Step 1: The Basics' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=1%'
GROUP BY 1,2

UNION ALL
--EGL Step 2--
SELECT day,'EGL Step 2: Institution Details' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=2%'
GROUP BY 1,2

UNION ALL
--EGL Step 3--
SELECT day,'EGL Step 3: Number of Seats' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=3%'
GROUP BY 1,2

UNION ALL
--EGL Application Details--
SELECT day,'EGL Application Details' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=applicationDetail%'
GROUP BY 1,2

UNION ALL
--EGL License Information--
SELECT day,'EGL License Information' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=licenseInformation%'
GROUP BY 1,2

UNION ALL
--EGL Request Seats--
SELECT day,'EGL Request Seats' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=editSeats%'
GROUP BY 1,2

UNION ALL
--EGL Renew Seats--
SELECT day,'EGL Renew Seats' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/education-grant-license%?page=renewSeats%'
GROUP BY 1,2

UNION ALL
--EGL Fail Application--
SELECT day,'EGL Application Failed!' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/%EGL/failed%'
GROUP BY 1,2
----------------------------------------------------------------------------
UNION ALL
--id.unity.com--
SELECT day,'id.unity.com' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/%conversations%' AND hostname='id.unity.com'
GROUP BY 1,2
----------------------------------------------------------------------------
UNION ALL
--Student Plan Fail Application--
SELECT day,'Student Plan Application Failed!' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/%verification/student/failed%'
GROUP BY 1,2

UNION ALL
--Student Plan Success Application--
SELECT day,'Student Plan Application Succeed!' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/%verification/student/success%'
GROUP BY 1,2

UNION ALL
--Educator Plan Fail Application--
SELECT day,'Educator Plan Application Failed!' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/%educator/failed%'
GROUP BY 1,2

UNION ALL
--Educator Plan Success Application--
SELECT day,'Educator Plan Application Succeed!' AS page,SUM(ga_user_count) AS users
FROM `unity-other-learn-prd.reynafeng.gaweb_view`
WHERE full_page_path LIKE '/%educator/licenseApplied%'
GROUP BY 1,2

