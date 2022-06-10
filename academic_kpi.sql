--Update Time: 5/23
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.academic_kpi` AS
WITH asset_store AS (
  SELECT DATE_TRUNC(submit_date,MONTH) AS purchase_month,
       SUM(amount_final_usd)*0.3 AS net_asset_store,
       SUM(amount_final_usd) AS gross_asset_store
  FROM `unity-other-learn-prd.reynafeng.asset_store`
  GROUP BY 1 
  ORDER BY 1 DESC
)

SELECT *,
       --previous year
       IF(NOT LAG(total_students) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(total_students) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), total_students) AS lag_year_total_students,
       IF(NOT LAG(total_school) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(total_school) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)),total_school) AS lag_year_total_school,
       IF(NOT LAG(total_educator) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(total_educator) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), total_educator) AS lag_year_total_educator,
       IF(NOT LAG(total_mau) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(total_mau) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), total_mau) AS lag_year_total_mau,
       IF(NOT LAG(net_asset_store) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) IS NULL, LAG(net_asset_store) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)), net_asset_store) AS lag_year_net_asset_store
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
       0 AS paying_school,
       IF(NOT C.monthly_rolling_seats_sum IS NULL, C.monthly_rolling_seats_sum,0) AS educator_ep,
       IF(NOT E.Educator_commnunity IS NULL, E.Educator_commnunity,0) AS educator_community,
       IF(NOT D.total_valid IS NULL, D.total_valid,0) AS uaa_members,
       IF(NOT D.uaa_revenue IS NULL, D.uaa_revenue,0) AS uaa_revenue,
       IF(NOT F.net_asset_store IS NULL, F.net_asset_store,0) AS net_asset_store,
       IF(NOT F.gross_asset_store IS NULL, F.gross_asset_store,0) AS gross_asset_store
FROM `unity-other-learn-prd.reynafeng.sp_academic_kpi` A
LEFT JOIN `unity-other-learn-prd.reynafeng.egl_academic_kpi` B ON A.visit_month=B.visit_month 
LEFT JOIN `unity-other-learn-prd.reynafeng.ep_academic_kpi` C ON A.visit_month=C.visit_month
LEFT JOIN `unity-other-learn-prd.reynafeng.opportunity_uaa` D ON A.visit_month=D.report_month
LEFT JOIN `unity-other-learn-prd.reynafeng.KPI_Google_Sheet` E ON A.visit_month=E.Month
LEFT JOIN asset_store F ON A.visit_month=F.purchase_month
WHERE A.visit_month>='2020-02-01'
ORDER BY 1 ASC) AS A
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) A
ORDER BY visit_month
