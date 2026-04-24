with commits as (
    SELECT *
    FROM {{ ref('stg_github__commits') }}
),

repos as (
    SELECT *
    FROM {{ref('stg_github__repos')}}
),

# リポジトリごとに月次集計をおこなう
monthly_summary as (
    SELECT
        repo_name,
        date_trunc(committer_date, month) as commit_month,
        COUNT(commit_hash) as commit_count,
        COUNT(DISTINCT committer_name) as unique_committers
    FROM
        commits
    GROUP BY
        1, 2
),

# 成長率を計算するための関数使用
joined as (
    SELECT
        m.repo_name,
        m.commit_month,
        m.commit_count,
        m.unique_committers,
        LAG(m.commit_count) OVER (PARTITION BY m.repo_name ORDER BY m.commit_month) as last_month_commit_count,
    FROM monthly_summary as m
)
# 成長率の計算
SELECT *,
    CASE 
        WHEN last_month_commit_count is NULL OR last_month_commit_count = 0  THEN  NULL
        ELSE SAFE_DIVIDE((commit_count - last_month_commit_count), last_month_commit_count)
    END AS commit_grow_rate
FROM
    joined