--Update time: 7/6
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.egl_expired_license` AS

SELECT license,institutionName,start_date,end_date,contactEmail,contactName,real_user_id,
       DATE_DIFF(end_date, start_date, month) AS duration_month,
       IF(end_date <= DATE_SUB(DATE_TRUNC(current_date,month), INTERVAL 1 MONTH),'WITHIN MONTH','BEYOND MONTH') AS time_type,
       DATE_DIFF(DATE(current_date),end_date,DAY) AS days_expire
FROM(
SELECT license_record_id ,license ,institutionName,request_time,expire_time,contactEmail,contactName,real_user_id,
       MIN(DATE(request_time)) OVER(PARTITION BY license_record_id) AS start_date,
       MAX(DATE(expire_time)) OVER(PARTITION BY license_record_id) AS end_date
FROM `unity-other-learn-prd.reynafeng.egl_grant_license`
WHERE status='Approved'
GROUP BY 1,2,3,4,5,6,7,8) AS A
ORDER BY 4 DESC