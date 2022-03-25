--Update Time: 3/10 8:36 PM--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_flow` AS

WITH nuo AS (
SELECT *
FROM(
    SELECT nuo.compliance_key, first_template_type, first_template_chosen, 
           ROW_NUMBER() OVER (PARTITION BY nuo.compliance_key ORDER BY first_template_chosen) AS rnk,
           MIN(first_hub_login) OVER(PARTITION BY nuo.compliance_key) AS first_hub_login,
           MIN(first_editor_download_end) OVER(PARTITION BY nuo.compliance_key) AS first_editor_download_end,
           MIN(first_editor_install_end) OVER(PARTITION BY nuo.compliance_key) AS first_editor_install_end,
           MIN(first_editor_login) OVER(PARTITION BY nuo.compliance_key) AS first_editor_login,
           MIN(nuo.n_first_walkthrough_completed+nuo.n_new_walkthrough_completed) OVER(PARTITION BY nuo.compliance_key) AS wt_completed_count,
           MIN(webgl_post_first_ts) OVER(PARTITION BY nuo.compliance_key) AS webgl_post_first_ts
    FROM `unity-other-liveplatform-prd.nuo.nuo_funnel_all_sessions` nuo
    JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` student ON nuo.compliance_key=student.user_id AND DATE(nuo.first_hub_login) BETWEEN grant_time AND expire_time) AS A
WHERE rnk=1),

user AS (
  SELECT A.compliance_key,A.machineid,A.license_hash
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` A
  JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` student ON A.compliance_key=student.user_id
  GROUP BY 1,2,3
),

hub_mg AS (
    SELECT COALESCE(mapping.compliance_key,user.compliance_key) AS compliance_key,
           MIN(mapping.submit_date) AS first_hub_login,
           MIN(templates.template_chosen) AS first_template_chosen
    FROM(
      SELECT *
      FROM `unity-ai-data-prd.hub_general.hub_general_start_v1`
      WHERE submit_date IS NOT NULL) mapping
    LEFT JOIN `unity-other-liveplatform-prd.nuo.nuo_templates` templates ON mapping.head.machineid=templates.machineid AND mapping.head.license_hash=templates.license_hash
    FULL OUTER JOIN user ON mapping.head.machineid=user.machineid AND mapping.head.license_hash=user.license_hash
    JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` student ON COALESCE(mapping.compliance_key,user.compliance_key)=student.user_id AND mapping.submit_date BETWEEN grant_time AND expire_time
    WHERE mapping.submit_date IS NOT NULL
    GROUP BY 1
),

editor AS (
    SELECT COALESCE(mapping.compliance_key,user.compliance_key) AS compliance_key,
           MIN(mapping.login_date) AS first_editor_login
    FROM `unity-other-liveplatform-prd.ontology.cml_daily_login` mapping
    FULL OUTER JOIN user ON mapping.machineid=user.machineid AND mapping.license_hash=user.license_hash
    JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` student ON COALESCE(mapping.compliance_key,user.compliance_key)=student.user_id AND mapping.login_date BETWEEN DATE(grant_time) AND DATE(expire_time)
    WHERE login_date IS NOT NULL
    GROUP BY 1
),

purchased AS (
    SELECT compliance_key,total_usd_user
    FROM `unity-other-learn-prd.reynafeng.asset_store`
    WHERE egl=True
    GROUP BY 1,2
),
    
downloads AS (
    SELECT compliance_key,total_downloads
    FROM `unity-other-learn-prd.reynafeng.asset_download`
    WHERE egl=True
    GROUP BY 1,2
)

SELECT student.license,student.grant_time,student.user_id,student.contactEmail,
       COALESCE(hub_mg.first_hub_login,DATE(nuo.first_hub_login)) AS first_hub_login,
       COALESCE(editor.first_editor_login,DATE(nuo.first_editor_login)) AS first_editor_login,
       nuo.first_editor_download_end,
       nuo.first_editor_install_end,
       nuo.wt_completed_count,
       nuo.webgl_post_first_ts,
       COALESCE(nuo.first_template_chosen, hub_mg.first_template_chosen) AS template_chosen,
       purchased.total_usd_user,
       downloads.total_downloads,
       MIN(CASE WHEN student.is_install=True THEN student.install_time ELSE NULL END) AS first_activation_ts
FROM `unity-other-learn-prd.reynafeng.egl_grant_license` student
LEFT JOIN nuo ON student.user_id = nuo.compliance_key
LEFT JOIN hub_mg ON student.user_id = hub_mg.compliance_key
LEFT JOIN editor ON student.user_id = editor.compliance_key
LEFT JOIN purchased ON student.user_id = purchased.compliance_key
LEFT JOIN downloads ON student.user_id = downloads.compliance_key
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13