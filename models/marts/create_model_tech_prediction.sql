-- models/marts/create_model_tech_prediction.sql

{{ config(
    materialized='table',
    post_hook=[
        "CREATE OR REPLACE MODEL `{{this.database}}.{{ this.schema }}.tech_prediction_model`
         OPTIONS(model_type='boosted_tree_regressor', input_label_cols=['target_growth_multiplier'])
         AS SELECT * FROM {{ this }}"
    ]
) }}

-- まずは学習用データをテーブルとして作成する
SELECT
    commit_growth_rate,
    commit_density,
    active_repos,
    target_growth_multiplier
FROM {{ref('train_tech_prediction_regression')}}
WHERE target_growth_multiplier IS NOT NULL
   
