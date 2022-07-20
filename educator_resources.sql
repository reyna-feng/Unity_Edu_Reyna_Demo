--Update Time: 6/14
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.educator_resources` AS 
--Educator Plan Reached
WITH educator_reached AS (
SELECT visit_month,
       educator_ep,educator_community,
       school_egl+school_edlab+paying_school AS school_represent
FROM `unity-other-learn-prd.reynafeng.academic_kpi`
ORDER BY 1 DESC
),

--Course Active Start
course_start AS (
SELECT DATE_TRUNC(DATE(start_time),MONTH) AS visit_month,
       COUNT(DISTINCT user_id) AS course_start
FROM(
SELECT user_id,start_time,license,IF(NOT started IS NULL, started,0) AS started
FROM `unity-other-liveplatform-prd.learn.learn_user_content`
WHERE content_title IN ("unity for educators: a beginner's guide","create with code - teacher training",
      "create with vr for educators","educators live",
      "zoe - vr for education","teaching game design and development")
      AND start_time IS NOT NULL AND soft_start_time IS NULL
GROUP BY 1,2,3,4
) A
GROUP BY 1
ORDER BY 1 DESC
),

--Project/Tutorial Active Start
project_start AS (
SELECT DATE_TRUNC(DATE(start_time),MONTH) AS visit_month,
       COUNT(DISTINCT user_id) AS project_start
FROM(
SELECT user_id,content_title,tier,content_topic,content_level,start_time,
       IF(NOT started IS NULL, started,0) AS started,
       IF(NOT soft_started IS NULL, soft_started,0) AS soft_started
FROM `unity-other-liveplatform-prd.learn.learn_user_content`
WHERE (project_title_ = 'getting started with playground: for educators'
      OR content_title = 'teach hour of code')
      AND start_time IS NOT NULL AND soft_start_time IS NULL
GROUP BY 1,2,3,4,5,6,7,8
) A
GROUP BY 1
ORDER BY 1 DESC
),

--Learn Educator Resource Downloads
file_download AS (
SELECT DATE_TRUNC(DATE(ts),MONTH) AS visit_month,COUNT(*) AS resource_downloads,
       COUNT(DISTINCT compliance_key) AS num_users
FROM(
SELECT COALESCE(head.user_id,compliance_key) AS compliance_key,body.ts
FROM `unity-ai-data-prd.learn_learner.learn_learner_fileDownload_v1` content_download
WHERE submit_date IS NOT NULL AND body.for_educators=true
      AND (body.file_name ='FAQ_Unity for Educators.pdf' OR 
           body.file_name ='Classroom Preparation Tips' OR
           body.file_name ='Complete Lesson Plans' OR
           body.file_name ='Create with Code - All Lesson Plans.pdf' OR
           body.file_name ='Live Session Links' OR
           body.file_name ='Teaching VR - Classroom Preparation Tips.pdf' OR 
           body.file_name ='Unity Project Strategy Guide.pdf' OR
           body.file_name LIKE 'All Lesson Plans%' OR
           body.file_name LIKE 'Create with VR%' OR
           body.file_name LIKE 'Creative Core Pathway Facilitator Kit%' OR
           body.file_name LIKE 'Curriculum%' OR 
           body.file_name LIKE 'CwC - %' OR 
           body.file_name LIKE 'Educator%' OR 
           body.file_name LIKE 'GD%' OR 
           body.file_name LIKE 'Getting Started%' OR 
           body.file_name LIKE 'Junior Programmer Pathway Facilitator Kit%' OR 
           body.file_name LIKE 'Lesson Plans%' OR 
           body.file_name LIKE 'Make a Spinner%' OR 
           body.file_name LIKE 'Scope%' OR 
           body.file_name LIKE 'Standards Alignment%' OR 
           body.file_name LIKE 'Syllabus%' OR 
           body.file_name LIKE 'Teacher%' OR 
           body.file_name LIKE 'Unit %' OR 
           body.file_name LIKE 'Unity Essentials Pathway Facilitator Kit%' OR 
           body.file_name LIKE 'Unity for Educators Course%')
      AND body.file_name!='Teacher Video.zip'
GROUP BY 1,2) AS A
GROUP BY 1
ORDER BY 1 DESC
),

--Curriculum Resource Downloads
curriculum_download AS (
SELECT DATE_TRUNC(date,MONTH) AS visit_month,SUM(visits) AS curriculum_download
FROM `unity-other-liveplatform-prd.acquisition_dashboard.raw_ga_session_data` 
WHERE DATE(date)>=DATE('2020-02-25')
      AND full_page_path LIKE '%thank-you-curricular-framework%'
GROUP BY 1
ORDER BY 1 DESC
),

--Live Training Registrations
live_training AS (
SELECT DATE_TRUNC(register_date,MONTH) AS visit_month,
       SUM(registered) AS live_training
FROM `unity-other-liveplatform-prd.learn.learn_user_live_session`
WHERE session_topic='For Educators' AND session_title NOT LIKE 'Educators Live%'
GROUP BY 1
ORDER BY 1 DESC
),

--Educators Live Webinar Registrations
educator_live AS (
SELECT DATE_TRUNC(date,MONTH) AS visit_month,COUNT(DISTINCT fullVisitorId) AS educator_live
FROM `unity-other-liveplatform-prd.acquisition_dashboard.raw_ga_session_data` 
WHERE DATE(date)>=DATE('2020-02-25')
      AND full_page_path LIKE '/educators-live%'
      AND full_page_path NOT IN ('/educators-live-2022','/educators-live-2021','/educators-live-2020')
      AND full_page_path NOT LIKE '%fbclid%'
      AND hostname IN('create.unity3d.com','create.unity.com')
GROUP BY 1
ORDER BY 1 DESC
),

--Unity Educator Workshop Registrations (Events, Sponsored)
educator_workshop AS(
SELECT Month,Educator_workshop AS educator_workshop
FROM `unity-other-learn-prd.reynafeng.KPI_Google_Sheet`
GROUP BY 1,2
ORDER BY 1 DESC
)

SELECT A.*,B.course_start,C.project_start,D.resource_downloads,E.curriculum_download,
       F.live_training,G.educator_live,H.educator_workshop
FROM educator_reached A
LEFT JOIN course_start B ON A.visit_month=B.visit_month
LEFT JOIN project_start C ON A.visit_month=C.visit_month
LEFT JOIN file_download D ON A.visit_month=D.visit_month
LEFT JOIN curriculum_download E ON A.visit_month=E.visit_month
LEFT JOIN live_training F ON A.visit_month=F.visit_month
LEFT JOIN educator_live G ON A.visit_month=G.visit_month
LEFT JOIN educator_workshop H ON A.visit_month=H.Month