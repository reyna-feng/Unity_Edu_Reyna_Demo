CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_flow` AS

WITH nuo AS (
SELECT *
FROM(
    SELECT nuo.compliance_key, first_template_type, first_template_chosen, 
           ROW_NUMBER() OVER (PARTITION BY nuo.compliance_key ORDER BY first_template_chosen) AS rnk,
           MIN(first_hub_login) OVER(PARTITION BY nuo.compliance_key) AS first_hub_login,
           MIN(first_editor_login) OVER(PARTITION BY nuo.compliance_key) AS first_editor_login,
           MIN(nuo.n_first_walkthrough_completed+nuo.n_new_walkthrough_completed) OVER(PARTITION BY nuo.compliance_key) AS wt_completed_count,
           MIN(webgl_post_first_ts) OVER(PARTITION BY nuo.compliance_key) AS webgl_post_first_ts
    FROM `unity-other-liveplatform-prd.nuo.nuo_funnel_all_sessions` nuo) AS A
WHERE rnk=1
),

hub_mg AS (
    SELECT mapping.compliance_key,
           MIN(mapping.first_login_date) AS first_hub_login,
           MIN(templates.template_chosen) AS first_template_chosen,
    FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` mapping
    LEFT JOIN `unity-other-liveplatform-prd.nuo.nuo_templates` templates ON mapping.machineid=templates.machineid AND mapping.license_hash=templates.license_hash
    GROUP BY 1
),

editor AS (
    SELECT mapping.compliance_key,MIN(mapping.first_login_date) AS first_editor_login
    FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` mapping
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
),

license_activate AS (
  SELECT compliance_key,MAX(body.ts) AS hub_license_activate
  FROM `unity-ai-data-prd.hub_general.hub_general_licenseActivate_v1` 
  WHERE submit_date IS NOT NULL
  GROUP BY 1 
)

SELECT student.*,
       COALESCE(DATE(nuo.first_hub_login), hub_mg.first_hub_login) AS first_hub_login,
       COALESCE(DATE(nuo.first_editor_login), editor.first_editor_login) AS first_editor_login,
       license_activate.hub_license_activate,
       nuo.wt_completed_count,
       nuo.webgl_post_first_ts,
       COALESCE(nuo.first_template_chosen, hub_mg.first_template_chosen) AS template_chosen,
       purchased.total_usd_user,
       downloads.total_downloads
FROM `unity-other-learn-prd.reynafeng.egl_grant_license` student
LEFT JOIN nuo ON student.user_id = nuo.compliance_key
LEFT JOIN hub_mg ON student.user_id = hub_mg.compliance_key
LEFT JOIN editor ON student.user_id = editor.compliance_key
LEFT JOIN purchased ON student.user_id = purchased.compliance_key
LEFT JOIN downloads ON student.user_id = downloads.compliance_key
LEFT JOIN license_activate ON license_activate.compliance_key = student.user_id AND DATE(license_activate.hub_license_activate) >= COALESCE(DATE(nuo.first_editor_login), editor.first_editor_login)