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

machine AS (
  SELECT machineid,license_hash,sessionid
  FROM `unity-other-liveplatform-prd.ontology.ml_session_mapping`
  WHERE machine_count_per_session = 1
  GROUP BY 1,2,3
),

user AS (
  SELECT A.compliance_key,A.machineid,A.license_hash
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` A
  JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` student ON A.compliance_key=student.user_id
  GROUP BY 1,2,3
),

hub_mg AS (
    SELECT mapping.compliance_key,
           MIN(mapping.login_date) AS first_hub_login,
           MIN(templates.template_chosen) AS first_template_chosen
    FROM (
      SELECT COALESCE(daily_logins.compliance_key,user.compliance_key) AS compliance_key, 
             daily_logins.login_date,
             COALESCE(daily_logins.machineid,user.machineid) AS machineid,
             COALESCE(daily_logins.license_hash,user.license_hash) AS license_hash
      FROM `unity-other-liveplatform-prd.ontology.cml_daily_login` daily_logins
      JOIN user ON user.machineid = daily_logins.machineid AND user.license_hash = daily_logins.license_hash
      GROUP BY 1,2,3,4
    ) mapping
    JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` student ON mapping.compliance_key=student.user_id AND mapping.login_date BETWEEN grant_time AND expire_time
    LEFT JOIN `unity-other-liveplatform-prd.nuo.nuo_templates` templates ON mapping.machineid=templates.machineid AND mapping.license_hash=templates.license_hash
    GROUP BY 1
),

editor AS (
    SELECT mapping.compliance_key,MIN(mapping.submit_date) AS first_editor_login
    FROM `unity-ai-data-prd.editor_analytics.editor_analytics_appRunning_v1` mapping
    LEFT JOIN machine ON head.sessionid = machine.sessionid AND head.machineid IS NULL
    JOIN user ON user.machineid = COALESCE(head.machineid, machine.machineid)
    JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` student ON mapping.compliance_key=student.user_id AND mapping.submit_date BETWEEN DATE(grant_time) AND DATE(expire_time)
    WHERE COALESCE(head.machineid, machine.machineid, user.machineid) IS NOT NULL
          AND COALESCE(head.license_hash, machine.license_hash, user.license_hash) IS NOT NULL
          AND submit_date IS NOT NULL
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

SELECT student.license,student.grant_time,student.user_id,
       COALESCE(DATE(nuo.first_hub_login), hub_mg.first_hub_login) AS first_hub_login,
       COALESCE(DATE(nuo.first_editor_login), editor.first_editor_login) AS first_editor_login,
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
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12