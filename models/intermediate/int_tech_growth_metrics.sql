with repo_activity as (
    SELECT * 
    FROM {{ref('int_repo_monthly_activity')}}
),

languages as (
    SELECT * 
    FROM {{ref('stg_github__languages')}}
),

# 言語ごとの月次集計
tech_monthly_agg as (
    SELECT
        l.language_name,
        a.commit_month,
        SUM(a.commit_count) AS total_commits,
        COUNT(DISTINCT a.repo_name) AS active_repos,
        COUNT(DISTINCT a.unique_committers) AS total_developers
    FROM
        repo_activity AS a
    INNER JOIN 
        languages AS l
        ON a.repo_name = l.repo_name
    GROUP BY 1,2
),

# 前月比較と移動平均での指標計算算出
tech_metrics as (
    SELECT *,
        # 前月のコミット数
        LAG(total_commits) OVER(PARTITION BY language_name ORDER BY commit_month) AS prev_month_commits,
        # 直近３か月の平均コミット数
        AVG(total_commits) OVER(
            PARTITION BY language_name
            ORDER BY commit_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_commits_3month
    FROM tech_monthly_agg
)

SELECT *,
    # 成長率
    CASE 
        WHEN prev_month_commits IS NULL OR prev_month_commits = 0 THEN NULL  
        ELSE SAFE_DIVIDE((total_commits - prev_month_commits), prev_month_commits)
    END AS commit_growth_rate,
    # コミット密度
    SAFE_DIVIDE(total_commits, total_developers) AS commit_density
FROM tech_metrics

