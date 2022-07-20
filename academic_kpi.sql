--Update Time: 7/19
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.academic_kpi` AS
WITH asset_store AS (
  SELECT DATE_TRUNC(submit_date,MONTH) AS purchase_month,
       SUM(amount_final_usd)*0.3 AS net_asset_store,
       SUM(amount_final_usd) AS gross_asset_store
  FROM `unity-other-learn-prd.reynafeng.asset_store`
  GROUP BY 1 
  ORDER BY 1 DESC
),

rev_pro AS (
SELECT *,
       SUM(paying_school) OVER(ORDER BY rnk RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_sum
FROM(
  SELECT report_month,COUNT(DISTINCT rc_line_customer_name) AS paying_school,
         SUM(r_ttl_rev_activity) AS offline_rev,
         SUM(CASE WHEN direct_resller='Direct' THEN r_ttl_rev_activity ELSE 0 END) AS direct_offline_rev,
         SUM(CASE WHEN direct_resller='Reseller' THEN r_ttl_rev_activity ELSE 0 END) AS reseller_offline_rev,
         ROW_NUMBER() OVER(ORDER BY report_month) AS rnk
  FROM `unity-other-learn-prd.reynafeng.edu_revpro`
  GROUP BY 1
  ORDER BY 1 DESC) A
),

subscription AS (
SELECT DATE_TRUNC(DATE(createdTime),MONTH) AS report_month,SUM(invoice_amount_confirmed_total) AS subscription_rev
FROM `unity-other-learn-prd.reynafeng.genesis_service`
GROUP BY 1
ORDER BY 1 desc
),

salesforce AS (
SELECT DATE_TRUNC(closedate,MONTH) AS report_month,SUM(ACV_USD) AS sf_acv,
       SUM(CASE WHEN Channel__c='Direct' THEN ACV_USD ELSE 0.0 END) AS direct_sf_acv,
       SUM(CASE WHEN Channel__c='Reseller' THEN ACV_USD ELSE 0.0 END) AS reseller_sf_acv
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
WHERE Stage_Category='Won' AND quoteline_start_date=opportunity_start_date AND DATE_TRUNC(closedate,YEAR)=DATE_TRUNC(opportunity_start_date,YEAR) AND Account_Owner='Israel Macias'
GROUP BY 1
ORDER BY 1 DESC
)

SELECT *,
       --previous year
       IF(NOT LAG(total_students) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(total_students) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), total_students) AS lag_year_total_students,
       IF(NOT LAG(total_school) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(total_school) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)),total_school) AS lag_year_total_school,
       IF(NOT LAG(total_educator) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(total_educator) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), total_educator) AS lag_year_total_educator,
       IF(NOT LAG(total_mau) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(total_mau) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), total_mau) AS lag_year_total_mau,
       IF(NOT LAG(net_asset_store) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(net_asset_store) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), net_asset_store) AS lag_year_net_asset_store,
       IF(NOT LAG(offline_rev) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(offline_rev) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), offline_rev) AS lag_year_offline_rev,
       IF(NOT LAG(subscription_rev) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(subscription_rev) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), offline_rev) AS lag_year_subscription_rev
FROM(
SELECT * EXCEPT(educator_mau,student_mau),
       student_sp+student_egl+student_edlab AS total_students,
       school_egl+school_edlab+paying_school AS total_school,
       educator_ep+educator_community+school_egl+school_edlab+paying_school AS total_educator,
       student_mau+educator_mau+egl_seats_mau AS total_mau,
       IF(school_egl>0,egl_monthly_institution/school_egl*(educator_ep+educator_community+school_egl+school_edlab+paying_school)+egl_monthly_institution+educator_mau,educator_mau) AS educator_mau,
       IF(school_egl>0,egl_seats_mau-(egl_monthly_institution/school_egl*(educator_ep+educator_community+school_egl+school_edlab+paying_school)+egl_monthly_institution)+student_mau,student_mau) AS student_mau
FROM(
SELECT A.visit_month,A.current_month,A.monthly_rolling_seats_sum AS student_sp,
       A.MAU AS student_mau,
       IF(NOT B.grantCount IS NULL, B.grantCount, 0) AS egl_granted,
       IF(NOT B.monthly_users IS NULL, B.monthly_users, 0) AS egl_mau,
       IF(NOT B.monthly_rolling_seats IS NULL, B.monthly_rolling_seats*B.activation_multiplier, 0) AS egl_seats_mau,
       IF(NOT B.monthly_institution IS NULL, B.monthly_institution, 0) AS egl_monthly_institution,
       IF(NOT C.MAU IS NULL, C.MAU, 0) AS educator_mau,
       IF(NOT B.egl_license_balance IS NULL,B.egl_school_balance,0) AS school_egl,
       3*IF(NOT B.monthly_rolling_seats_sum IS NULL,B.monthly_rolling_seats_sum,0) AS student_egl,
       0 AS student_edlab,
       0 AS school_edlab,
       IF(NOT G.rolling_sum IS NULL, G.rolling_sum*0.75,0) AS paying_school,
       IF(NOT G.offline_rev IS NULL, G.offline_rev,0)+IF(NOT H.revenue IS NULL, H.revenue,0) AS offline_rev,
       IF(NOT G.direct_offline_rev IS NULL, G.direct_offline_rev,0) AS direct_offline_rev,
       IF(NOT G.reseller_offline_rev IS NULL, G.reseller_offline_rev,0) AS reseller_offline_rev,
       IF(NOT K.subscription_rev IS NULL, K.subscription_rev,0) AS subscription_rev,
       IF(NOT C.monthly_rolling_seats_sum IS NULL, C.monthly_rolling_seats_sum,0) AS educator_ep,
       IF(NOT J.members IS NULL AND J.name='Unity Teach Community', J.members,0) AS educator_community,
       IF(NOT D.uaa_revenue IS NULL, D.uaa_revenue,0) AS uaa_revenue,
       IF(NOT F.net_asset_store IS NULL, F.net_asset_store,0) AS net_asset_store,
       IF(NOT F.gross_asset_store IS NULL, F.gross_asset_store,0) AS gross_asset_store,
       IF(NOT I.sf_acv IS NULL, I.sf_acv,0) AS salesforce_acv,
       IF(NOT I.direct_sf_acv IS NULL, I.direct_sf_acv,0) AS salesforce_direct_acv,
       IF(NOT I.reseller_sf_acv IS NULL, I.reseller_sf_acv,0) AS salesforce_reseller_acv
FROM `unity-other-learn-prd.reynafeng.sp_academic_kpi` A
LEFT JOIN `unity-other-learn-prd.reynafeng.egl_academic_kpi` B ON A.visit_month=B.visit_month 
LEFT JOIN `unity-other-learn-prd.reynafeng.ep_academic_kpi` C ON A.visit_month=C.visit_month
LEFT JOIN `unity-other-learn-prd.reynafeng.opportunity_uaa` D ON A.visit_month=D.report_month
--LEFT JOIN `unity-other-learn-prd.reynafeng.KPI_Google_Sheet` E ON A.visit_month=E.Month
LEFT JOIN asset_store F ON A.visit_month=F.purchase_month
LEFT JOIN rev_pro G ON G.report_month=A.visit_month
LEFT JOIN subscription K ON K.report_month=A.visit_month
LEFT JOIN `unity-other-learn-prd.reynafeng.RevWd_Accruals` H ON H.report_month=A.visit_month
LEFT JOIN salesforce I ON I.report_month=A.visit_month
LEFT JOIN `unity-other-learn-prd.reynafeng.facebook_group_insights` J ON DATE_TRUNC(J.refresh_date,MONTH)=A.visit_month
WHERE A.visit_month>='2020-02-01'
ORDER BY 1 ASC) AS A
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
) A
ORDER BY visit_month