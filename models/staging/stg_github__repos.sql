select
    repo_name,
    watch_count as star_count
from {{ source('github_public', 'sample_repos') }}