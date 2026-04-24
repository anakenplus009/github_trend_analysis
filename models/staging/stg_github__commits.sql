with source as (
    select * from {{ source('github_public', 'sample_commits') }}
),

renamed as (
    select
        -- リポジトリの特定（複数のリポジトリ名が含まれる配列を1つに展開）
        repo_name as repo_name,
        
        -- コミット者の情報
        committer.name as committer_name,
        
        -- 日時情報の整理（BigQueryのTIMESTAMP型を扱いやすくする）
        committer.date as committer_date,
        
        -- コミットメッセージ（解析には使わないが、データの性質確認用に一応保持）
        message,
        
        -- コミットの一意のハッシュ
        commit as commit_hash

    from source
    where committer.date >= '2010-04-01'
)

select * from renamed