-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS getdata.raw;
CREATE SCHEMA IF NOT EXISTS getdata.calculated;

CREATE OR REPLACE TABLE getdata.raw.hr_job_change_train AS
SELECT *
FROM read_files(
  '/Volumes/getdata/raw/hr_job_change/aug_train.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE getdata.raw.hr_job_change_test AS
SELECT *
FROM read_files(
  '/Volumes/getdata/raw/hr_job_change/aug_test.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE getdata.raw.hr_job_change_sample_submission AS
SELECT *
FROM read_files(
  '/Volumes/getdata/raw/hr_job_change/sample_submission.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

SELECT 'hr_job_change_train' AS table_name, COUNT(*) AS row_count FROM getdata.raw.hr_job_change_train
UNION ALL
SELECT 'hr_job_change_test', COUNT(*) FROM getdata.raw.hr_job_change_test
UNION ALL
SELECT 'hr_job_change_sample_submission', COUNT(*) FROM getdata.raw.hr_job_change_sample_submission;

DESCRIBE HISTORY getdata.raw.hr_job_change_train;

-- COMMAND ----------

CREATE OR REPLACE TABLE getdata.calculated.stg_hr_candidates AS
SELECT
    enrollee_id,

    TRIM(city)                                             AS city,
    city_development_index,

    COALESCE(NULLIF(TRIM(gender), ''), 'Unknown')          AS gender,

    relevent_experience,
    CASE
        WHEN relevent_experience = 'Has relevent experience' THEN 1
        ELSE 0
    END                                                     AS has_relevant_experience_flag,

    COALESCE(enrolled_university, 'Unknown')               AS enrolled_university,
    COALESCE(education_level, 'Unknown')                    AS education_level,
    COALESCE(major_discipline, 'Not Specified')             AS major_discipline,

    CASE
        WHEN experience = '<1'  THEN 0
        WHEN experience = '>20' THEN 21
        WHEN experience IS NULL THEN NULL
        ELSE CAST(experience AS INT)
    END                                                     AS experience_years,

    CASE
        WHEN company_size = '<10'      THEN 5
        WHEN company_size = '10/49'    THEN 30
        WHEN company_size = '50-99'    THEN 75
        WHEN company_size = '100-500'  THEN 300
        WHEN company_size = '500-999'  THEN 750
        WHEN company_size = '1000-4999' THEN 3000
        WHEN company_size = '5000-9999' THEN 7500
        WHEN company_size = '10000+'   THEN 10000
        ELSE NULL
    END                                                     AS company_size_midpoint,
    COALESCE(company_size, 'Unknown')                       AS company_size_bucket,
    COALESCE(company_type, 'Unknown')                       AS company_type,

    CASE
        WHEN last_new_job = 'never' THEN 0
        WHEN last_new_job = '>4'    THEN 5
        WHEN last_new_job IS NULL   THEN NULL
        ELSE CAST(last_new_job AS INT)
    END                                                     AS years_since_last_job,

    training_hours,
    target,
    CASE
        WHEN target = 1.0 THEN 'Looking for change'
        WHEN target = 0.0 THEN 'Not looking'
        ELSE 'Unknown'
    END                                                     AS candidate_status,

    CURRENT_TIMESTAMP()                                     AS loaded_at
FROM getdata.raw.hr_job_change_train;

SELECT
    COUNT(*)                                                       AS total_rows,
    SUM(CASE WHEN gender = 'Unknown' THEN 1 ELSE 0 END)            AS gender_filled_in,
    SUM(CASE WHEN experience_years IS NULL THEN 1 ELSE 0 END)      AS experience_still_null,
    ROUND(AVG(training_hours), 1)                                  AS avg_training_hours
FROM getdata.calculated.stg_hr_candidates;


-- COMMAND ----------

CREATE OR REPLACE TABLE getdata.calculated.dim_city AS
SELECT
    ROW_NUMBER() OVER (ORDER BY city)                          AS city_key,
    city,
    city_development_index,
    CASE
        WHEN city_development_index >= 0.8 THEN 'High Development'
        WHEN city_development_index >= 0.6 THEN 'Medium Development'
        ELSE 'Low Development'
    END                                                          AS development_tier
FROM (
    SELECT DISTINCT city, city_development_index
    FROM getdata.calculated.stg_hr_candidates
) AS distinct_cities;

CREATE OR REPLACE TABLE getdata.calculated.dim_education AS
SELECT
    ROW_NUMBER() OVER (ORDER BY education_level, major_discipline) AS education_key,
    education_level,
    major_discipline
FROM (
    SELECT DISTINCT education_level, major_discipline
    FROM getdata.calculated.stg_hr_candidates
) AS distinct_education;

CREATE OR REPLACE TABLE getdata.calculated.dim_company AS
SELECT
    ROW_NUMBER() OVER (ORDER BY company_type, company_size_bucket) AS company_key,
    company_type,
    company_size_bucket,
    company_size_midpoint
FROM (
    SELECT DISTINCT company_type, company_size_bucket, company_size_midpoint
    FROM getdata.calculated.stg_hr_candidates
) AS distinct_company;

CREATE OR REPLACE TABLE getdata.calculated.fact_job_seeking AS
SELECT
    s.enrollee_id,
    c.city_key,
    e.education_key,
    co.company_key,
    s.gender,
    s.has_relevant_experience_flag,
    s.enrolled_university,
    s.experience_years,
    s.years_since_last_job,
    s.training_hours,
    s.target,
    s.candidate_status
FROM getdata.calculated.stg_hr_candidates AS s
INNER JOIN getdata.calculated.dim_city AS c
    ON s.city = c.city
   AND s.city_development_index = c.city_development_index
LEFT JOIN getdata.calculated.dim_education AS e
    ON s.education_level = e.education_level
   AND s.major_discipline = e.major_discipline
LEFT JOIN getdata.calculated.dim_company AS co
    ON s.company_type = co.company_type
   AND s.company_size_bucket = co.company_size_bucket;

CREATE OR REPLACE VIEW getdata.calculated.vw_fact_enriched AS
SELECT
    f.enrollee_id,
    ct.city,
    ct.city_development_index,
    ct.development_tier,
    ed.education_level,
    ed.major_discipline,
    cm.company_type,
    cm.company_size_bucket,
    cm.company_size_midpoint,
    f.gender,
    f.has_relevant_experience_flag,
    f.enrolled_university,
    f.experience_years,
    f.years_since_last_job,
    f.training_hours,
    f.target,
    f.candidate_status
FROM getdata.calculated.fact_job_seeking AS f
INNER JOIN getdata.calculated.dim_city      AS ct ON f.city_key      = ct.city_key
LEFT JOIN  getdata.calculated.dim_education AS ed ON f.education_key = ed.education_key
LEFT JOIN  getdata.calculated.dim_company   AS cm ON f.company_key   = cm.company_key;

SELECT
    (SELECT COUNT(*) FROM getdata.calculated.stg_hr_candidates)  AS staging_rows,
    (SELECT COUNT(*) FROM getdata.calculated.fact_job_seeking)   AS fact_rows,
    (SELECT COUNT(*) FROM getdata.calculated.vw_fact_enriched)   AS view_rows;


-- COMMAND ----------

SELECT
    development_tier,
    COUNT(*)                       AS candidate_count,
    ROUND(AVG(training_hours), 1)  AS avg_training_hours,
    ROUND(AVG(target) * 100, 1)    AS pct_looking_for_change
FROM getdata.calculated.vw_fact_enriched
GROUP BY development_tier
HAVING COUNT(*) > 100
   AND AVG(training_hours) > 60
ORDER BY avg_training_hours DESC;

SELECT
    development_tier,
    major_discipline,
    looking_for_change_count,
    RANK() OVER (
        PARTITION BY development_tier
        ORDER BY looking_for_change_count DESC
    ) AS rank_in_tier
FROM (
    SELECT
        development_tier,
        major_discipline,
        COUNT(*) AS looking_for_change_count
    FROM getdata.calculated.vw_fact_enriched
    WHERE target = 1.0
    GROUP BY development_tier, major_discipline
) AS agg
QUALIFY rank_in_tier <= 3
ORDER BY development_tier, rank_in_tier;

WITH experience_summary AS (
    SELECT
        experience_years,
        COUNT(*)                           AS candidate_count,
        ROUND(AVG(target) * 100, 1)        AS pct_looking_for_change
    FROM getdata.calculated.vw_fact_enriched
    WHERE experience_years IS NOT NULL
    GROUP BY experience_years
)
SELECT
    experience_years,
    candidate_count,
    pct_looking_for_change,
    LAG(pct_looking_for_change)  OVER (ORDER BY experience_years) AS prev_year_pct,
    ROUND(
        pct_looking_for_change
        - LAG(pct_looking_for_change) OVER (ORDER BY experience_years)
    , 1)                                                          AS pct_change_vs_prev_year,
    LEAD(pct_looking_for_change) OVER (ORDER BY experience_years) AS next_year_pct
FROM experience_summary
ORDER BY experience_years;

WITH city_summary AS (
    SELECT
        ct.city,
        COUNT(*)                       AS candidate_count,
        SUM(f.training_hours)          AS total_training_hours
    FROM getdata.calculated.fact_job_seeking AS f
    INNER JOIN getdata.calculated.dim_city AS ct ON f.city_key = ct.city_key
    GROUP BY ct.city
)
SELECT
    city,
    candidate_count,
    total_training_hours,
    ROW_NUMBER() OVER (ORDER BY candidate_count DESC)              AS city_rank,
    SUM(total_training_hours) OVER (
        ORDER BY candidate_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                                AS running_total_hours
FROM city_summary
ORDER BY city_rank
LIMIT 20;

WITH education_avg AS (
    SELECT
        education_level,
        AVG(training_hours) AS avg_hours_for_level
    FROM getdata.calculated.vw_fact_enriched
    GROUP BY education_level
)
SELECT
    v.enrollee_id,
    v.education_level,
    v.training_hours,
    ea.avg_hours_for_level,
    v.candidate_status
FROM getdata.calculated.vw_fact_enriched AS v
INNER JOIN education_avg AS ea
    ON v.education_level = ea.education_level
WHERE v.training_hours > ea.avg_hours_for_level
ORDER BY v.training_hours DESC
LIMIT 50;

SELECT DISTINCT
    education_level,
    major_discipline,
    CONCAT(UPPER(education_level), ' – ', major_discipline)      AS segment_label,
    LENGTH(major_discipline)                                      AS discipline_name_length,
    REPLACE(company_type, ' ', '_')                               AS company_type_slug
FROM getdata.calculated.vw_fact_enriched
ORDER BY education_level, major_discipline;

SELECT
    experience_years,
    FLOOR(experience_years / 5.0) * 5                              AS experience_bucket_start,
    CEIL((experience_years + 1) / 5.0) * 5                         AS experience_bucket_end,
    ROUND(AVG(city_development_index), 3)                          AS avg_city_dev_index,
    ABS(ROUND(AVG(target) - 0.5, 3))                                AS deviation_from_midpoint
FROM getdata.calculated.vw_fact_enriched
WHERE experience_years IS NOT NULL
GROUP BY experience_years
ORDER BY experience_years;

SELECT city, city_development_index, 'train' AS source_dataset
FROM getdata.raw.hr_job_change_train

UNION

SELECT city, city_development_index, 'test' AS source_dataset
FROM getdata.raw.hr_job_change_test
ORDER BY city;

CREATE OR REPLACE TEMP VIEW tmp_high_potential_candidates AS
SELECT enrollee_id, city, education_level, training_hours, target
FROM getdata.calculated.vw_fact_enriched
WHERE has_relevant_experience_flag = 1
  AND target = 1.0
  AND training_hours > 100;

SELECT COUNT(*) AS high_potential_candidate_count
FROM tmp_high_potential_candidates;

SELECT
    DATE(loaded_at)                       AS load_date,
    DATE_FORMAT(loaded_at, 'yyyy-MM')     AS load_year_month,
    COUNT(*)                              AS rows_loaded,
    COALESCE(SUM(target), 0)              AS total_looking_for_change
FROM getdata.calculated.stg_hr_candidates
GROUP BY DATE(loaded_at), DATE_FORMAT(loaded_at, 'yyyy-MM');


-- COMMAND ----------

CREATE OR REPLACE VIEW getdata.calculated.vw_dashboard_overview AS
SELECT
    enrollee_id,
    city,
    development_tier,
    education_level,
    major_discipline,
    company_type,
    company_size_bucket,
    gender,
    CASE WHEN has_relevant_experience_flag = 1 THEN 'Has experience' ELSE 'No experience' END AS relevant_experience_label,
    enrolled_university,
    experience_years,
    years_since_last_job,
    training_hours,
    target,
    candidate_status
FROM getdata.calculated.vw_fact_enriched;

CREATE OR REPLACE VIEW getdata.calculated.vw_dashboard_city_metrics AS
SELECT
    ct.city,
    ct.development_tier,
    ct.city_development_index,
    COUNT(*)                                                       AS candidate_count,
    ROUND(AVG(f.training_hours), 1)                                AS avg_training_hours,
    ROUND(AVG(f.target) * 100, 1)                                  AS pct_looking_for_change,
    RANK() OVER (ORDER BY COUNT(*) DESC)                           AS city_size_rank
FROM getdata.calculated.fact_job_seeking AS f
INNER JOIN getdata.calculated.dim_city AS ct ON f.city_key = ct.city_key
GROUP BY ct.city, ct.development_tier, ct.city_development_index;

CREATE OR REPLACE VIEW getdata.calculated.vw_dashboard_education_experience AS
SELECT
    education_level,
    major_discipline,
    CASE
        WHEN experience_years IS NULL THEN 'Unknown'
        WHEN experience_years < 3   THEN '0-2 years'
        WHEN experience_years < 6   THEN '3-5 years'
        WHEN experience_years < 11  THEN '6-10 years'
        WHEN experience_years < 21  THEN '11-20 years'
        ELSE '21+ years'
    END                                                             AS experience_bucket,
    COUNT(*)                                                        AS candidate_count,
    ROUND(AVG(target) * 100, 1)                                     AS pct_looking_for_change
FROM getdata.calculated.vw_fact_enriched
GROUP BY
    education_level,
    major_discipline,
    CASE
        WHEN experience_years IS NULL THEN 'Unknown'
        WHEN experience_years < 3   THEN '0-2 years'
        WHEN experience_years < 6   THEN '3-5 years'
        WHEN experience_years < 11  THEN '6-10 years'
        WHEN experience_years < 21  THEN '11-20 years'
        ELSE '21+ years'
    END;

CREATE OR REPLACE VIEW getdata.calculated.vw_dashboard_company_profile AS
SELECT
    company_type,
    company_size_bucket,
    company_size_midpoint,
    COUNT(*)                                                        AS candidate_count,
    ROUND(AVG(target) * 100, 1)                                     AS pct_looking_for_change,
    ROUND(AVG(training_hours), 1)                                   AS avg_training_hours,
    DENSE_RANK() OVER (ORDER BY AVG(target) DESC)                   AS turnover_risk_rank
FROM getdata.calculated.vw_fact_enriched
WHERE company_type <> 'Unknown'
GROUP BY company_type, company_size_bucket, company_size_midpoint;

CREATE OR REPLACE VIEW getdata.calculated.vw_dashboard_top_flight_risk_segments AS
WITH segment_summary AS (
    SELECT
        development_tier,
        education_level,
        company_type,
        COUNT(*)                       AS candidate_count,
        AVG(target)                    AS avg_target
    FROM getdata.calculated.vw_fact_enriched
    GROUP BY development_tier, education_level, company_type
    HAVING COUNT(*) >= 30
)
SELECT
    development_tier,
    education_level,
    company_type,
    candidate_count,
    ROUND(avg_target * 100, 1)                                     AS pct_looking_for_change,
    ROW_NUMBER() OVER (ORDER BY avg_target DESC)                   AS risk_rank
FROM segment_summary
QUALIFY risk_rank <= 15
ORDER BY risk_rank;



-- COMMAND ----------

