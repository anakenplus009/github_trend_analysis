{{ config(materialized='table') }}

WITH latest_features AS(
    SELECT * --今月の技術指標の獲得
    FROM 
        {{ref('int_tech_growth_metrics')}}
    WHERE
        -- テーブル内の最新の月を自動で特定
        TIMESTAMP(commit_month) = (SELECT MAX(TIMESTAMP(commit_month)) FROM {{ref('int_tech_growth_metrics')}})
)


SELECT
    language_name,
    commit_month,
    commit_growth_rate,
    commit_density,
    active_repos,
    predicted_target_growth_multiplier AS predicted_growth_factor
FROM
    ML.PREDICT(
        MODEL `noted-cortex-460601-c3.dev_marts.tech_prediction_model`, --モデルIDを指定
            (SELECT * FROM latest_features)
    )
ORDER BY
    predicted_growth_factor DESC