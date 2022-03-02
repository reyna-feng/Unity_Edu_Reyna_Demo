CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.student_flow` AS

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
    JOIN `unity-other-learn-prd.reynafeng.student_activation` student ON nuo.compliance_key=student.compliance_key AND nuo.first_hub_login BETWEEN licnese_grant_time AND licnese_expiration_time) AS A
WHERE rnk=1
),

hub_mg AS (
    SELECT mapping.compliance_key,
           MIN(mapping.first_login_date) AS first_hub_login,
           MIN(templates.template_chosen) AS first_template_chosen,
    FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` mapping
    JOIN `unity-other-learn-prd.reynafeng.student_activation` student ON mapping.compliance_key=student.compliance_key AND mapping.first_login_date BETWEEN DATE(licnese_grant_time) AND DATE(licnese_expiration_time)
    LEFT JOIN `unity-other-liveplatform-prd.nuo.nuo_templates` templates ON mapping.machineid=templates.machineid AND mapping.license_hash=templates.license_hash
    GROUP BY 1
),

editor AS (
    SELECT mapping.compliance_key,MIN(mapping.first_login_date) AS first_editor_login
    FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` mapping
    JOIN `unity-other-learn-prd.reynafeng.student_activation` student ON mapping.compliance_key=student.compliance_key AND mapping.first_login_date BETWEEN DATE(licnese_grant_time) AND DATE(licnese_expiration_time)
    GROUP BY 1
),

purchased AS (
    SELECT compliance_key,total_usd_user
    FROM `unity-other-learn-prd.reynafeng.asset_store`
    WHERE sp=True
    GROUP BY 1,2
),
    
downloads AS (
    SELECT compliance_key,total_downloads
    FROM `unity-other-learn-prd.reynafeng.asset_download`
    WHERE sp=True
    GROUP BY 1,2
)

SELECT student.*,
       COALESCE(DATE(nuo.first_hub_login), hub_mg.first_hub_login) AS first_hub_login,
       COALESCE(DATE(nuo.first_editor_login), editor.first_editor_login) AS first_editor_login,
       nuo.first_editor_download_end,
       nuo.first_editor_install_end,
       nuo.wt_completed_count,
       nuo.webgl_post_first_ts,
       COALESCE(nuo.first_template_chosen, hub_mg.first_template_chosen) AS template_chosen,
       purchased.total_usd_user,
       downloads.total_downloads
FROM `unity-other-learn-prd.reynafeng.student_activation` student
LEFT JOIN nuo ON student.compliance_key = nuo.compliance_key
LEFT JOIN hub_mg ON student.compliance_key = hub_mg.compliance_key
LEFT JOIN editor ON student.compliance_key = editor.compliance_key
LEFT JOIN purchased ON student.compliance_key = purchased.compliance_key
LEFT JOIN downloads ON student.compliance_key = downloads.compliance_key