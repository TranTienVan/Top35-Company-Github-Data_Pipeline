
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

select
    l.language,
    sum(used_count) as number_of_used,
    count(distinct r.id) as number_of_repositories
from {{source("github_profile", "REPOSITORY_LANGUAGE")}} as l
inner join {{source("github_profile", "REPOSITORY")}} as r
    on r.id = l.repository_id
group by l.language
order by 
    sum(used_count) desc,
    count(distinct r.id) desc


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
