
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

select
    u.name,
    u.html_url,
    u.blog,
    u.location,
    count(c.sha) as number_of_commits,
    count(distinct r.id) as number_of_repositories
from {{source("github_profile", "USER")}} as u
inner join {{source("github_profile", "COMMIT")}} as c
    on c.author_id = u.id
inner join {{source("github_profile", "REPOSITORY")}} as r
    on c.repository_id = r.id
group by 
    u.name,
    u.html_url,
    u.blog,
    u.location
order by
    count(c.sha) desc,
    count(distinct r.id) desc


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
