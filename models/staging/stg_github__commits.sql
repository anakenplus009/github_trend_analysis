{{
    config(
        materialized='incremental',
        unique_key= 'commit_hash',
        partition_by={
            "field":"committer_date",
            "data_type":"timestamp",
            "granularity":"month"
        }
    )
}}

with source as (
    select * from {{ source('github_public', 'commits') }}
    where 1=1
    {% if is_incremental() %}
      AND TIMESTAMP_SECONDS(committer.date.seconds) > (SELECT max(committer_date) FROM {{this}})
    {% else %}
      AND timestamp_seconds(committer.date.seconds) >= '2020-01-01'
    {% endif %}
),

renamed as (
    select
        -- リポジトリの特定（複数のリポジトリ名が含まれる配列を1つに展開）
        repo_name as repo_name,
        
        -- コミット者の情報
        committer.name as committer_name,
        
        -- 日時情報の整理（BigQueryのTIMESTAMP型を扱いやすくする）
        TIMESTAMP_SECONDS(committer.date.seconds) as committer_date,
        
        -- コミットメッセージ（解析には使わないが、データの性質確認用に保持）
        message,
        
        -- コミットの一意のハッシュ
        commit as commit_hash

    from source
    where TIMESTAMP_SECONDS(committer.date.seconds) <= current_timestamp()
)

select * from renamed
