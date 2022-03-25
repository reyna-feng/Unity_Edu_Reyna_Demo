--Update Time: 3/24--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.gaweb_view` AS
SELECT day,hostname,full_page_path,
       COUNT(DISTINCT fullvisitorid) ga_user_count,
       SUM(visits) ga_session_count,
       SUM(purchase_count) ga_purchase_count,
       AVG(session_duration) ga_avg_session_duration,
       SUM(CASE WHEN bounces>=1 THEN visits END)/SUM(visits) *100 ga_bounce_rate
FROM(
SELECT
    date AS day,
    fullvisitorid,
    hostname,
    full_page_path,
    session_id,
    COUNT(transaction_id) purchase_count,
    SUM(next_time-time) session_duration,
    AVG(visits) visits,
    SUM(bounces) bounces
FROM `unity-other-liveplatform-prd.acquisition_dashboard.raw_ga_session_data` 
WHERE --XTRACT(YEAR FROM CURRENT_DATE())-EXTRACT(YEAR FROM DATE(date))<=1 AND
      DATE(date)>=DATE('2020-02-25')
      AND hostname IN ('unity.com','learn.unity.com','id.unity.com')
GROUP BY 1,2,3,4,5
) AS A
GROUP BY 1,2,3