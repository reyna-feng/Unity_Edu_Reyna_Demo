--Update time: 6/8
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.egl_expired_license` AS

SELECT license,institutionName,start_month,end_month,contactEmail,
       DATE_DIFF(end_month, start_month, month) AS duration_month,
       IF(end_month <= DATE_SUB(DATE_TRUNC(current_date,month), INTERVAL 1 MONTH),'WITHIN MONTH','BEYOND MONTH') AS time_type
FROM(
SELECT license_record_id ,license ,institutionName,request_time,expire_time,contactEmail,
       MIN(DATE_TRUNC(request_time, month)) OVER(PARTITION BY license_record_id) AS start_month,
       MAX(DATE_TRUNC(expire_time, month)) OVER(PARTITION BY license_record_id) AS end_month
FROM `unity-other-learn-prd.reynafeng.egl_grant_license`
WHERE status='Approved'
GROUP BY 1,2,3,4,5,6) AS A
ORDER BY 4 DESC