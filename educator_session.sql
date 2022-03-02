CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.educator_session` AS

--One compliance key one date one row
WITH session_cnt AS(
      SELECT compliance_key,session_start_date,count(1) as sessions_count,
             DATE_DIFF(session_start_date, LAG(session_start_date) OVER(PARTITION BY compliance_key ORDER BY session_start_date), day) AS days_between_activedays
      FROM `unity-other-learn-prd.reynafeng.educator_running`
      WHERE hrs_between_sessions>=0.25 OR hrs_between_sessions IS NULL
      GROUP BY 1,2
)

SELECT compliance_key,
       session_start_date,
       session_start_week,
       session_start_month,
       sessions_count,
       country_code_most_freq,
       email,license,
       last_active_date,
       session_user_time_hrs,
       CAST(COUNT(DISTINCT compliance_key) OVER(PARTITION BY session_start_date) AS INT64) AS DAU,
       CAST(COUNT(DISTINCT compliance_key) OVER(PARTITION BY session_start_week) AS INT64) AS WAU,
       CAST(COUNT(DISTINCT compliance_key) OVER(PARTITION BY session_start_month) AS INT64) AS MAU,
       SUM(sessions_count) OVER(PARTITION BY session_start_date) AS daily_tot_sessions_cnt,
       SUM(sessions_count) OVER(PARTITION BY session_start_week) AS weekly_tot_sessions_cnt,
       SUM(sessions_count) OVER(PARTITION BY session_start_month) AS monthly_tot_sessions_cnt,
       SUM(session_user_time_hrs) OVER(PARTITION BY session_start_date) AS daily_tot_sessions_time,
       SUM(session_user_time_hrs) OVER(PARTITION BY session_start_week) AS weekly_tot_sessions_time,
       SUM(session_user_time_hrs) OVER(PARTITION BY session_start_month) AS monthly_tot_sessions_time,
FROM(
SELECT sessions.compliance_key,
       sessions.session_start_date,
       DATE_TRUNC(sessions.session_start_date,month) AS session_start_month,
       sessions.session_start_week,
       s_cnt.sessions_count,
       country_code_most_freq,email,license,
       MAX(sessions.session_start_date) OVER(PARTITION BY sessions.compliance_key) AS last_active_date,
       SUM(session_user_time_hrs) AS session_user_time_hrs
FROM `unity-other-learn-prd.reynafeng.educator_running` AS sessions
JOIN session_cnt AS s_cnt ON sessions.compliance_key=s_cnt.compliance_key AND sessions.session_start_date=s_cnt.session_start_date
GROUP BY 1,2,3,4,5,6,7,8) AS A
GROUP BY 1,2,3,4,5,6,7,8,9,10