
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

select 
    r.name,
    r.full_name,
    r.html_url,
    count(c.sha) as number_of_commits,
    count(distinct c.author_id) as number_of_contributors,
    count(l.language) as number_of_languages,
    count(t.commit_sha) as number_of_tags
from {{source("github_profile", "REPOSITORY")}} as r
inner join {{source("github_profile", "COMMIT")}} as c
    on c.repository_id = r.id
inner join {{source("github_profile", "REPOSITORY_LANGUAGE")}} as l
    on l.repository_id = r.id
inner join {{source("github_profile", "REPOSITORY_TAG")}} as t
    on t.commit_sha = c.sha
group by 
    r.name, 
    r.full_name, 
    r.html_url
order by 
    count(c.sha) desc, 
    count(distinct c.author_id) desc, 
    count(l.language) desc, 
    count(t.commit_sha) desc




/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
