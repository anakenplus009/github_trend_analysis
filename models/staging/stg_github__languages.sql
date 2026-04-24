select
    repo_name,
    l.name as language_name,
    l.bytes as language_bytes
from {{ source('github_public', 'languages') }},
UNNEST(language) as l  -- 配列を展開して1行1言語にする