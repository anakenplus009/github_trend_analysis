{{ config(materialized='table') }}

WITH tech_metrics AS (
    SELECT *
    FROM {{ref('int_tech_growth_metrics')}}
),

target_values AS (
    SELECT
        commit_growth_rate,
        commit_density,
        active_repos,
        rolling_avg_commits_3month,
        # 正解ラベル：三か月後の成長倍率の算出
        SAFE_DIVIDE(
            LEAD(total_commits, 3) OVER(PARTITION BY language_name ORDER BY commit_month), 
            total_commits) AS target_growth_multiplier
    FROM
        tech_metrics 
)

SELECT *
FROM target_values
WHERE
    target_growth_multiplier IS NOT NULL
    AND target_growth_multiplier < 100