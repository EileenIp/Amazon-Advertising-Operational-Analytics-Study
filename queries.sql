--Jobs Reviewed Over Time
--(Calculating the number of jobs reviewed per hour for each day in November 2020)
SELECT 
    DATE_TRUNC('hour', ds) AS review_hour,
    DATE_TRUNC('day', ds) AS review_date,
    COUNT(*) AS num_jobs_reviewed
FROM
    performance
WHERE
    DATE_TRUNC('month', ds) = '2020-11-01'::DATE
GROUP BY
    DATE_TRUNC('hour', ds),
    DATE_TRUNC('day', ds)
ORDER BY
    review_date,
    review_hour;

--Throughput Analysis:
--Calculating the 7-day rolling average of throughput (number of events per second)
WITH DAILY_METRIC AS (
    SELECT
        ds,
        COUNT(job_id) AS job_review
    FROM
        job_data
    GROUP BY
        ds
)
SELECT
    ds,
    job_review,
    AVG(job_review) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS throughput
FROM
    DAILY_METRIC
ORDER BY
    throughput DESC;

--C.Language Share Analysis:
--Calculating the percentage share of each language in the last 30 days.
SELECT 
    language,
    COUNT(language) AS language_count,
    (COUNT(language) / (SELECT COUNT(*) FROM job_data)) * 100 AS percentage_share
FROM job_data
GROUP BY language
ORDER BY language DESC;

--D.Duplicate Rows Detection:
--Identifying duplicate rows in the data.
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY JOB_ID) AS DUPLICATE_ROWS
    FROM JOB_DATA
) AS A_R
WHERE DUPLICATE_ROWS > 1;

--Weekly User Engagement:
--Measuring the activeness of users on a weekly basis.

SELECT
    DATE_TRUNC('week', created_at) AS week_start_date,
    COUNT(DISTINCT user_id) AS active_users_count
FROM
    users
GROUP BY
    week_start_date
ORDER BY
    week_start_date;


--User Growth Analysis:
--Analyzing the growth of users over time for a product.

SELECT
    DATE_TRUNC('month', created_at) AS month_start_date,
    COUNT(DISTINCT user_id) AS total_users
FROM
    users
GROUP BY
    month_start_date
ORDER BY
    month_start_date;

--Weekly Retention Analysis:
--Analyzing the retention of users on a weekly basis after signing up for a product.
WITH user_signups AS (
    SELECT
        user_id,
        DATE_TRUNC('week', created_at) AS signup_week
    FROM
        users
),
user_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('week', occurred_at) AS activity_week
    FROM
        events
)
SELECT
    us.signup_week AS cohort_week,
    ua.activity_week AS retention_week,
    COUNT(DISTINCT ua.user_id) AS retained_users
FROM
    user_signups us
LEFT JOIN
    user_activity ua ON us.user_id = ua.user_id AND ua.activity_week >= us.signup_week
GROUP BY
    us.signup_week, ua.activity_week
ORDER BY
    us.signup_week, ua.activity_week;


--D.Weekly Engagement Per Device:
--Measuring the activeness of users on a weekly basis per device.
SELECT
    DATE_TRUNC('week', e.occurred_at) AS week_start_date,
    e.device,
    COUNT(DISTINCT e.user_id) AS active_users_count
FROM
    events e
GROUP BY
    week_start_date, e.device
ORDER BY
    week_start_date, e.device;


--E.Email Engagement Analysis:
--Analyzing how users are engaging with the email service.

SELECT
    action,
    COUNT(DISTINCT user_id) AS unique_users_count,
    COUNT(*) AS total_actions_count
FROM
    email_events
GROUP BY
    action
ORDER BY
    action;
