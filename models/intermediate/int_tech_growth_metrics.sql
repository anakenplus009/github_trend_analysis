with repo_activity as (
    SELECT * 
    FROM {{ref('int_repo_monthly_activity')}}
),

languages as (
    SELECT
      repo_name,
      language_name,
      language_bytes,
      -- リポジトリごとの合計bytesに対する各言語の割合を算出
      SAFE_DIVIDE(language_bytes, SUM(language_bytes) OVER(PARTITION BY repo_name)) as lang_share_ratio  
    FROM {{ref('stg_github__languages')}}
),

# 言語ごとの月次集計
tech_monthly_agg as (
    SELECT
        l.language_name,
        a.commit_month,
        -- コミット数に言語割合を掛けて合計（加重平均）
        SUM(a.commit_count * l.lang_share_ratio) AS total_commits,
        -- アクティブなリポジトリ数は、その言語が使われている「実質的な数」として合計
        SUM(l.lang_share_ratio) AS active_repos_weighted,
        -- 開発者数も割合で按分
        SUM(a.unique_committers * l.lang_share_ratio) AS total_developers_weighted
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

